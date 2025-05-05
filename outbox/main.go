package usecase

import (
	"context"
	"errors"
	"fmt"
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
type OrderRepository interface {
	CreateOrder(ctx context.Context, order *Order) error
	UpdateOrderStatus(ctx context.Context, orderID int, status string) error
}

type MessageBroker interface {
	PublishOrderCreated(ctx context.Context, event *OrderEvent) error
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

	// tx.begin
	// defer tx.rollback

	if err := uc.repo.CreateOrder(ctx, order); err != nil {
		return fmt.Errorf("failed to create order: %v", err)
	}

	// Публикуем событие в брокер сообщений
	event := &OrderEvent{
		OrderID: order.ID,
		UserID:  userID,
		Amount:  amount,
		Status:  "created",
	}

	if err := uc.broker.PublishOrderCreated(ctx, event); err != nil {
		return fmt.Errorf("failed to publish order event: %v", err)
	}

	// tx.commit

	return nil
}
