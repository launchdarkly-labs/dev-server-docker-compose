package datasourcev2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/launchdarkly/go-server-sdk/v7/internal/fdv2proto"

	"context"

	es "github.com/launchdarkly/eventsource"

	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldtime"
	ldevents "github.com/launchdarkly/go-sdk-events/v3"
	"github.com/launchdarkly/go-server-sdk/v7/interfaces"
	"github.com/launchdarkly/go-server-sdk/v7/internal"
	"github.com/launchdarkly/go-server-sdk/v7/internal/datasource"
	"github.com/launchdarkly/go-server-sdk/v7/internal/endpoints"
	"github.com/launchdarkly/go-server-sdk/v7/subsystems"

	"golang.org/x/exp/maps"
)

const (
	streamReadTimeout        = 5 * time.Minute // the LaunchDarkly stream should send a heartbeat comment every 3 minutes
	streamMaxRetryDelay      = 30 * time.Second
	streamRetryResetInterval = 60 * time.Second
	streamJitterRatio        = 0.5
	defaultStreamRetryDelay  = 1 * time.Second

	streamingErrorContext     = "in stream connection"
	streamingWillRetryMessage = "will retry"
)

// Implementation of the streaming data source, not including the lower-level SSE implementation which is in
// the eventsource package.
//
// Error handling works as follows:
// 1. If any event is malformed, we must assume the stream is broken and we may have missed updates. Set the
// data source state to INTERRUPTED, with an error kind of INVALID_DATA, and restart the stream.
// 2. If we try to put updates into the data store and we get an error, we must assume something's wrong with the
// data store. We don't have to log this error because it is logged by DataSourceUpdateSinkImpl, which will also set
// our state to INTERRUPTED for us.
// 2a. If the data store supports status notifications (which all persistent stores normally do), then we can
// assume it has entered a failed state and will notify us once it is working again. If and when it recovers, then
// it will tell us whether we need to restart the stream (to ensure that we haven't missed any updates), or
// whether it has already persisted all of the stream updates we received during the outage.
// 2b. If the data store doesn't support status notifications (which is normally only true of the in-memory store)
// then we don't know the significance of the error, but we must assume that updates have been lost, so we'll
// restart the stream.
// 3. If we receive an unrecoverable error like HTTP 401, we close the stream and don't retry, and set the state
// to OFF. Any other HTTP error or network error causes a retry with backoff, with a state of INTERRUPTED.
// 4. We set the Future returned by start() to tell the client initialization logic that initialization has either
// succeeded (we got an initial payload and successfully stored it) or permanently failed (we got a 401, etc.).
// Otherwise, the client initialization method may time out but we will still be retrying in the background, and
// if we succeed then the client can detect that we're initialized now by calling our Initialized method.

// StreamProcessor is the internal implementation of the streaming data source.
//
// This type is exported from internal so that the StreamingDataSourceBuilder tests can verify its
// configuration. All other code outside of this package should interact with it only via the
// DataSource interface.
type StreamProcessor struct {
	cfg                        datasource.StreamConfig
	dataDestination            subsystems.DataDestination
	statusReporter             subsystems.DataSourceStatusReporter
	client                     *http.Client
	headers                    http.Header
	diagnosticsManager         *ldevents.DiagnosticsManager
	loggers                    ldlog.Loggers
	isInitialized              internal.AtomicBoolean
	halt                       chan struct{}
	connectionAttemptStartTime ldtime.UnixMillisecondTime
	connectionAttemptLock      sync.Mutex
	readyOnce                  sync.Once
	closeOnce                  sync.Once
}

// NewStreamProcessor creates the internal implementation of the streaming data source.
func NewStreamProcessor(
	context subsystems.ClientContext,
	dataDestination subsystems.DataDestination,
	statusReporter subsystems.DataSourceStatusReporter,
	cfg datasource.StreamConfig,
) *StreamProcessor {
	sp := &StreamProcessor{
		dataDestination: dataDestination,
		statusReporter:  statusReporter,
		headers:         context.GetHTTP().DefaultHeaders,
		loggers:         context.GetLogging().Loggers,
		halt:            make(chan struct{}),
		cfg:             cfg,
	}
	if cci, ok := context.(*internal.ClientContextImpl); ok {
		sp.diagnosticsManager = cci.DiagnosticsManager
	}

	sp.client = context.GetHTTP().CreateHTTPClient()
	// Client.Timeout isn't just a connect timeout, it will break the connection if a full response
	// isn't received within that time (which, with the stream, it never will be), so we must make
	// sure it's zero and not the usual configured default. What we do want is a *connection* timeout,
	// which is set by Config.newHTTPClient as a property of the Dialer.
	sp.client.Timeout = 0

	return sp
}

//nolint:revive // DataInitializer method.
func (sp *StreamProcessor) Name() string {
	return "StreamingDataSourceV2"
}

//nolint:revive // DataInitializer method.
func (sp *StreamProcessor) Fetch(_ context.Context) (*subsystems.Basis, error) {
	return nil, errors.New("StreamProcessor does not implement Fetch capability")
}

//nolint:revive // no doc comment for standard method
func (sp *StreamProcessor) IsInitialized() bool {
	return sp.isInitialized.Get()
}

//nolint:revive // DataSynchronizer method.
func (sp *StreamProcessor) Sync(closeWhenReady chan<- struct{}, selector fdv2proto.Selector) {
	sp.loggers.Info("Starting LaunchDarkly streaming connection")
	go sp.subscribe(closeWhenReady, selector)
}

//nolint:gocyclo
func (sp *StreamProcessor) consumeStream(stream *es.Stream, closeWhenReady chan<- struct{}) {
	// Consume remaining Events and Errors so we can garbage collect
	defer func() {
		for range stream.Events {
		} // COVERAGE: no way to cause this condition in unit tests
		if stream.Errors != nil {
			for range stream.Errors { // COVERAGE: no way to cause this condition in unit tests
			}
		}
	}()

	changeSetBuilder := fdv2proto.NewChangeSetBuilder()

	for {
		select {
		case event, ok := <-stream.Events:
			if !ok {
				// COVERAGE: stream.Events is only closed if the EventSource has been closed. However, that
				// only happens when we have received from sp.halt, in which case we return immediately
				// after calling stream.Close(), terminating the for loop-- so we should not actually reach
				// this point. Still, in case the channel is somehow closed unexpectedly, we do want to
				// terminate the loop.
				return
			}

			sp.logConnectionResult(true)

			//nolint:godox
			// TODO(cwaldren/mkeeler): Should this actually be true by default? It means if we receive an event
			// we don't understand then we go to the Valid state.
			processedEvent := true
			shouldRestart := false

			gotMalformedEvent := func(event es.Event, err error) {
				// The protocol should "forget" anything that happens upon receiving an error.
				changeSetBuilder = fdv2proto.NewChangeSetBuilder()

				if event == nil {
					sp.loggers.Errorf(
						"Received streaming events with malformed JSON data (%s); will restart stream",
						err,
					)
				} else {
					sp.loggers.Errorf(
						"Received streaming \"%s\" event with malformed JSON data (%s); will restart stream",
						event.Event(),
						err,
					)
				}

				errorInfo := interfaces.DataSourceErrorInfo{
					Kind:    interfaces.DataSourceErrorKindInvalidData,
					Message: err.Error(),
					Time:    time.Now(),
				}
				sp.statusReporter.UpdateStatus(interfaces.DataSourceStateInterrupted, errorInfo)

				shouldRestart = true // scenario 1 in error handling comments at top of file
				processedEvent = false
			}

			switch fdv2proto.EventName(event.Event()) {
			case fdv2proto.EventHeartbeat:
				// Swallow the event and move on.
			case fdv2proto.EventServerIntent:

				var serverIntent fdv2proto.ServerIntent
				err := json.Unmarshal([]byte(event.Data()), &serverIntent)
				if err != nil {
					gotMalformedEvent(event, err)
					break
				}

				// IntentNone is a special case where we won't receive a payload-transferred event, so we will need
				// to instead immediately notify the client that we are initialized.
				if serverIntent.Payload.Code == fdv2proto.IntentNone {
					sp.setInitializedAndNotifyClient(true, closeWhenReady)
					break
				}

				if err := changeSetBuilder.Start(serverIntent); err != nil {
					gotMalformedEvent(event, err)
					break
				}

			case fdv2proto.EventPutObject:
				var p fdv2proto.PutObject
				err := json.Unmarshal([]byte(event.Data()), &p)
				if err != nil {
					gotMalformedEvent(event, err)
					break
				}
				changeSetBuilder.AddPut(p.Kind, p.Key, p.Version, p.Object)
			case fdv2proto.EventDeleteObject:
				var d fdv2proto.DeleteObject
				err := json.Unmarshal([]byte(event.Data()), &d)
				if err != nil {
					gotMalformedEvent(event, err)
					break
				}
				changeSetBuilder.AddDelete(d.Kind, d.Key, d.Version)
			case fdv2proto.EventGoodbye:
				var goodbye fdv2proto.Goodbye
				err := json.Unmarshal([]byte(event.Data()), &goodbye)
				if err != nil {
					gotMalformedEvent(event, err)
					break
				}

				if !goodbye.Silent {
					sp.loggers.Errorf("SSE server received error: %s (%v)", goodbye.Reason, goodbye.Catastrophe)
				}
			case fdv2proto.EventError:
				var errorData fdv2proto.Error
				err := json.Unmarshal([]byte(event.Data()), &errorData)
				if err != nil {
					gotMalformedEvent(event, err)
					break
				}

				sp.loggers.Errorf("Error on %s: %s", errorData.PayloadID, errorData.Reason)

				// The protocol should "forget" anything that has happened, and expect that we will receive
				// more messages in the future (starting with a server intent.)
				changeSetBuilder = fdv2proto.NewChangeSetBuilder()
			case fdv2proto.EventPayloadTransferred:
				var selector fdv2proto.Selector
				err := json.Unmarshal([]byte(event.Data()), &selector)
				if err != nil {
					gotMalformedEvent(event, err)
					break
				}

				// After calling Finish, the builder is ready to receive a new changeset.
				changeSet, err := changeSetBuilder.Finish(selector)
				if err != nil {
					gotMalformedEvent(nil, err)
					break
				}

				code := changeSet.IntentCode()
				switch code {
				case fdv2proto.IntentTransferFull:
					sp.dataDestination.SetBasis(changeSet.Changes(), changeSet.Selector(), true)
				case fdv2proto.IntentTransferChanges:
					sp.dataDestination.ApplyDelta(changeSet.Changes(), changeSet.Selector(), true)
				case fdv2proto.IntentNone:
					/* We don't expect to receive this, but it could be possible. In that case, it should be
					equivalent to transferring no changes - a no-op.
					*/
				}

				sp.setInitializedAndNotifyClient(true, closeWhenReady)

			default:
				sp.loggers.Infof("Unexpected event found in stream: %s", event.Event())
			}

			if processedEvent {
				sp.statusReporter.UpdateStatus(interfaces.DataSourceStateValid, interfaces.DataSourceErrorInfo{})
			}
			if shouldRestart {
				stream.Restart()
			}

		case <-sp.halt:
			stream.Close()
			return
		}
	}
}

func (sp *StreamProcessor) subscribe(closeWhenReady chan<- struct{}, selector fdv2proto.Selector) {
	path := endpoints.AddPath(sp.cfg.URI, endpoints.StreamingRequestPath)
	if selector.IsDefined() {
		path = path + "?basis=" + selector.State()
	}
	req, reqErr := http.NewRequest("GET", path, nil)
	if reqErr != nil {
		sp.loggers.Errorf(
			"Unable to create a stream request; this is not a network problem, most likely a bad base URI: %s",
			reqErr,
		)
		sp.statusReporter.UpdateStatus(interfaces.DataSourceStateOff, interfaces.DataSourceErrorInfo{
			Kind:    interfaces.DataSourceErrorKindUnknown,
			Message: reqErr.Error(),
			Time:    time.Now(),
		})
		sp.logConnectionResult(false)
		close(closeWhenReady)
		return
	}
	if sp.cfg.FilterKey != "" {
		req.URL.RawQuery = url.Values{
			"filter": {sp.cfg.FilterKey},
		}.Encode()
	}
	if sp.headers != nil {
		req.Header = maps.Clone(sp.headers)
	}
	sp.loggers.Info("Connecting to LaunchDarkly stream")

	sp.logConnectionStarted()

	initialRetryDelay := sp.cfg.InitialReconnectDelay
	if initialRetryDelay <= 0 { // COVERAGE: can't cause this condition in unit tests
		initialRetryDelay = defaultStreamRetryDelay
	}

	errorHandler := func(err error) es.StreamErrorHandlerResult {
		sp.logConnectionResult(false)

		if se, ok := err.(es.SubscriptionError); ok {
			errorInfo := interfaces.DataSourceErrorInfo{
				Kind:       interfaces.DataSourceErrorKindErrorResponse,
				StatusCode: se.Code,
				Time:       time.Now(),
			}
			recoverable := checkIfErrorIsRecoverableAndLog(
				sp.loggers,
				httpErrorDescription(se.Code),
				streamingErrorContext,
				se.Code,
				streamingWillRetryMessage,
			)
			if recoverable {
				sp.logConnectionStarted()
				sp.statusReporter.UpdateStatus(interfaces.DataSourceStateInterrupted, errorInfo)
				return es.StreamErrorHandlerResult{CloseNow: false}
			}
			sp.statusReporter.UpdateStatus(interfaces.DataSourceStateOff, errorInfo)
			return es.StreamErrorHandlerResult{CloseNow: true}
		}

		checkIfErrorIsRecoverableAndLog(
			sp.loggers,
			err.Error(),
			streamingErrorContext,
			0,
			streamingWillRetryMessage,
		)
		errorInfo := interfaces.DataSourceErrorInfo{
			Kind:    interfaces.DataSourceErrorKindNetworkError,
			Message: err.Error(),
			Time:    time.Now(),
		}
		sp.statusReporter.UpdateStatus(interfaces.DataSourceStateInterrupted, errorInfo)
		sp.logConnectionStarted()
		return es.StreamErrorHandlerResult{CloseNow: false}
	}

	stream, err := es.SubscribeWithRequestAndOptions(req,
		es.StreamOptionHTTPClient(sp.client),
		es.StreamOptionReadTimeout(streamReadTimeout),
		es.StreamOptionInitialRetry(initialRetryDelay),
		es.StreamOptionUseBackoff(streamMaxRetryDelay),
		es.StreamOptionUseJitter(streamJitterRatio),
		es.StreamOptionRetryResetInterval(streamRetryResetInterval),
		es.StreamOptionErrorHandler(errorHandler),
		es.StreamOptionCanRetryFirstConnection(-1),
		es.StreamOptionLogger(sp.loggers.ForLevel(ldlog.Info)),
	)

	if err != nil {
		sp.logConnectionResult(false)

		close(closeWhenReady)
		return
	}

	sp.consumeStream(stream, closeWhenReady)
}

func (sp *StreamProcessor) setInitializedAndNotifyClient(success bool, closeWhenReady chan<- struct{}) {
	if success {
		wasAlreadyInitialized := sp.isInitialized.GetAndSet(true)
		if !wasAlreadyInitialized {
			sp.loggers.Info("LaunchDarkly streaming is active")
		}
	}
	sp.readyOnce.Do(func() {
		close(closeWhenReady)
	})
}

func (sp *StreamProcessor) logConnectionStarted() {
	sp.connectionAttemptLock.Lock()
	defer sp.connectionAttemptLock.Unlock()
	sp.connectionAttemptStartTime = ldtime.UnixMillisNow()
}

func (sp *StreamProcessor) logConnectionResult(success bool) {
	sp.connectionAttemptLock.Lock()
	startTimeWas := sp.connectionAttemptStartTime
	sp.connectionAttemptStartTime = 0
	sp.connectionAttemptLock.Unlock()

	if startTimeWas > 0 && sp.diagnosticsManager != nil {
		timestamp := ldtime.UnixMillisNow()
		sp.diagnosticsManager.RecordStreamInit(timestamp, !success, uint64(timestamp-startTimeWas))
	}
}

//nolint:revive // no doc comment for standard method
func (sp *StreamProcessor) Close() error {
	sp.closeOnce.Do(func() {
		close(sp.halt)
		sp.statusReporter.UpdateStatus(interfaces.DataSourceStateOff, interfaces.DataSourceErrorInfo{})
	})
	return nil
}

// GetBaseURI returns the configured streaming base URI, for testing.
func (sp *StreamProcessor) GetBaseURI() string {
	return sp.cfg.URI
}

// GetInitialReconnectDelay returns the configured reconnect delay, for testing.
func (sp *StreamProcessor) GetInitialReconnectDelay() time.Duration {
	return sp.cfg.InitialReconnectDelay
}

// GetFilterKey returns the configured key, for testing.
func (sp *StreamProcessor) GetFilterKey() string {
	return sp.cfg.FilterKey
}

// vim: foldmethod=marker foldlevel=0
