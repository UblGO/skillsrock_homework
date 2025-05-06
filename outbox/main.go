package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
type Tx interface {
	Rollback() error
	Commit() error
}

type TxManager interface {
	Begin(ctx context.Context) (Tx, error)
}

type OrderRepository interface {
	CreateOrder(ctx context.Context, tx Tx, order *Order) error
	CreateOutboxEvent(ctx context.Context, tx Tx, event *OrderEvent) error
	UpdateOrderStatus(ctx context.Context, tx Tx, orderID int, status string) error
	//возвращает слайс non-published ивентов
	GetUnpublishedEvents(ctx context.Context, tx Tx) ([]OrderEvent, error)
	//делает UPDATE ивента, меняя статус на published
	SetEventAsPublished(ctx context.Context, tx Tx, event *OrderEvent) error
	// Очистка outbox
	CleanupOutdatedEvents(ctx context.Context, tx Tx, retention time.Duration) error
}

type MessageBroker interface {
	PublishOrderCreated(ctx context.Context, event *OrderEvent) error
}

type Config struct {
	PollRate        time.Duration
	CleanupInterval time.Duration
	RetentionPeriod time.Duration
}

type usecase struct {
	log    slog.Logger
	config Config
	repo   OrderRepository
	broker MessageBroker
	tx     TxManager
}

func NewUsecase(repo *OrderRepository, broker MessageBroker, config Config) *usecase {
	return &usecase{
		config: config,
		repo:   *repo,
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
	tx, err := uc.tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := uc.repo.CreateOrder(ctx, tx, order); err != nil {
		return fmt.Errorf("failed to create order: %w", err)
	}
	event := &OrderEvent{
		OrderID: order.ID,
		UserID:  userID,
		Amount:  amount,
		Status:  "created",
	}
	// Помещаем ивент в outbox
	if err := uc.repo.CreateOutboxEvent(ctx, tx, event); err != nil {
		return fmt.Errorf("failed to create event in outbox table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (uc *usecase) RunOutboxProcessor(ctx context.Context) error {
	pollErrChan := make(chan error)
	cleanupErrChan := make(chan error)

	go func() {
		defer close(pollErrChan)
		if err := uc.runOutboxPoller(ctx); err != nil {
			pollErrChan <- err
		}
	}()

	go func() {
		defer close(cleanupErrChan)
		if err := uc.runOutdatedEventsCleanup(ctx); err != nil {
			cleanupErrChan <- err
		}
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-pollErrChan:
		uc.log.Error(err.Error())
	case err := <-cleanupErrChan:
		uc.log.Error(err.Error())
	}
	return nil
}

// функция вызывающая pollOutbox раз в PollRate
func (uc *usecase) runOutboxPoller(ctx context.Context) error {
	pollTicker := time.NewTicker(uc.config.PollRate)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pollTicker.C:
			if err := uc.pollOutbox(ctx); err != nil {
				return fmt.Errorf("failed to poll outbox: %w", err)
			}
		}
	}
}

// функция собирает non-published ивенты, отправляет в брокер, меняет статус на published
func (uc *usecase) pollOutbox(ctx context.Context) error {
	tx, err := uc.tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction when polling outbox: %w", err)
	}

	defer tx.Rollback()

	events, err := uc.repo.GetUnpublishedEvents(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to get unpublished events: %w", err)
	}

	for _, event := range events {
		if err := uc.broker.PublishOrderCreated(ctx, &event); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}
		if err := uc.repo.SetEventAsPublished(ctx, tx, &event); err != nil {
			return fmt.Errorf("failed to update event status: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction when polling outbox: %w", err)
	}
	return nil
}

// функция вызывающая cleanupOutdatedEvents раз в CleanupInterval
func (uc *usecase) runOutdatedEventsCleanup(ctx context.Context) error {
	cleanUpTicker := time.NewTicker(uc.config.CleanupInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-cleanUpTicker.C:
			if err := uc.cleanupOutdatedEvents(ctx); err != nil {
				return err
			}
		}
	}
}

// функция очищающая таблицу outbox от published ивентов, у которых время жизни >= RetentionPeriod
func (uc *usecase) cleanupOutdatedEvents(ctx context.Context) error {
	tx, err := uc.tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction when cleaning up: %w", err)
	}
	defer tx.Rollback()
	if err := uc.repo.CleanupOutdatedEvents(ctx, tx, uc.config.RetentionPeriod); err != nil {
		return fmt.Errorf("failed to clean up events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction when cleaning up: %w", err)
	}
	return nil
}
