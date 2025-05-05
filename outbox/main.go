package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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
type TxManager interface {
	Begin(ctx context.Context) error
	Rollback()
	Commit(ctx context.Context) error
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
}

type Config struct {
	PollRate time.Duration
}

type usecase struct {
	config Config
	repo   OrderRepository
	broker MessageBroker
	tx     TxManager
}

func NewUsecase(repo OrderRepository, broker MessageBroker, config Config) *usecase {
	return &usecase{
		config: config,
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
	err := uc.tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer uc.tx.Rollback()

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

	err = uc.tx.Commit(ctx)

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// функция переодически запускающая опрос outbox на наличие non-published ивентов; Запускается в отдельной горутине
func (uc *usecase) RunOutboxPoller(ctx context.Context) error {
	ticker := time.NewTicker(uc.config.PollRate)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := uc.pollOutbox(ctx); err != nil {
				return err
			}
		}
	}

}

// функция собирает non-published ивенты, отправляет в брокер, меняет статус на published
func (uc *usecase) pollOutbox(ctx context.Context) error {
	events, err := uc.repo.GetUnpublishedEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unpublished events: %w", err)
	}

	for _, event := range events {
		if err := uc.broker.PublishOrderCreated(ctx, &event); err != nil {
			return fmt.Errorf("failed to publish event: %v", err)
		}
		if err := uc.repo.SetEventAsPublished(ctx, &event); err != nil {
			return fmt.Errorf("failed to update event status: %v", err)
		}
	}
	return nil
}
