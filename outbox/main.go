package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Store struct {
	repo   OrderRepository
	broker MessageBroker
}
type Order struct {
	ID     int
	UserID int
	Amount float64
	Status string
}

type OrderEvent struct {
	OrderID int
	UserID  int
	Amount  float64
	Status  string
}
type OrderRepository interface {
	CreateOrder(ctx context.Context, order *Order) error
	CreateOutboxEvent(ctx context.Context, event *OrderEvent) error
	UpdateOrderStatus(ctx context.Context, orderID int, status string) error
	//возвращает слайс non-published ивентов
	GetUnpublishedEvents(ctx context.Context) ([]OrderEvent, error)
	//делает UPDATE ивента, меняя статус на published
	SetEventAsPublished(ctx context.Context, event *OrderEvent) error
}

type MessageBroker interface {
	PublishOrderCreated(ctx context.Context, event *OrderEvent) error
	PollOutbox(ctx context.Context, broker MessageBroker, repo OrderRepository) error
}

type usecase struct {
	repo   OrderRepository
	broker MessageBroker
}

func NewUsecase(repo OrderRepository, broker MessageBroker) *usecase {
	return &usecase{
		repo:   repo,
		broker: broker,
	}
}

func (uc *usecase) CreateOrder(ctx context.Context, userID int, amount float64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}

	// Создаем заказ
	order := &Order{
		UserID: userID,
		Amount: amount,
		Status: "created",
	}

	// tx.Begin()
	// defer tx.rollback

	if err := uc.repo.CreateOrder(ctx, order); err != nil {
		return fmt.Errorf("failed to create order: %v", err)
	}
	event := &OrderEvent{
		OrderID: order.ID,
		UserID:  userID,
		Amount:  amount,
		Status:  "created",
	}
	// Помещаем ивент в outbox
	if err := uc.repo.CreateOutboxEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to create event in outbox table: %v", err)
	}

	// tx.commit

	return nil
}

// При использовании функций ниже в качестве методов структуры более высокого уровня (store) брокер и дб не передаются в качестве аргументов

// функция переодически запускающая опрос outbox на наличие non-published ивентов; Запускается в отдельной горутине
func RunOutboxPoller(ctx context.Context, broker MessageBroker, repo OrderRepository) error {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := broker.PollOutbox(ctx, broker, repo); err != nil {
				return err
			}
		}
	}

}

// функция собирает non-published ивенты, отправляет в брокер, меняет статус на published
func PollOutbox(ctx context.Context, broker MessageBroker, repo OrderRepository) error {
	events, err := repo.GetUnpublishedEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unpublished events: %w", err)
	}

	for _, event := range events {
		if err := broker.PublishOrderCreated(ctx, &event); err != nil {
			return fmt.Errorf("failed to publish event: %v", err)
		}
		if err := repo.SetEventAsPublished(ctx, &event); err != nil {
			return fmt.Errorf("failed to update event status: %v", err)
		}
	}
	return nil
}
