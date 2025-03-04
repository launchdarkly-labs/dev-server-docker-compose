package subsystems

import (
	"io"

	"github.com/launchdarkly/go-server-sdk/v7/subsystems/ldstoretypes"
)

// DataStore is an interface for a data store that holds feature flags and related data received by
// the SDK.
//
// Ordinarily, the only implementations of this interface are the default in-memory implementation,
// which holds references to actual SDK data model objects, and the persistent data store
// implementation that delegates to a PersistentDataStore.
type DataStore interface {
	io.Closer

	ReadOnlyStore

	// Init overwrites the store's contents with a set of items for each collection.
	//
	// All previous data should be discarded, regardless of versioning.
	//
	// The update should be done atomically. If it cannot be done atomically, then the store
	// must first add or update each item in the same order that they are given in the input
	// data, and then delete any previously stored items that were not in the input data.
	Init(allData []ldstoretypes.Collection) error

	// Upsert updates or inserts an item in the specified collection. For updates, the object will only be
	// updated if the existing version is less than the new version.
	//
	// The SDK may pass an ItemDescriptor whose Item is nil, to represent a placeholder for a deleted
	// item. In that case, assuming the version is greater than any existing version of that item, the
	// store should retain that placeholder rather than simply not storing anything.
	//
	// The method returns true if the item was updated, or false if it was not updated because the store
	// contains an equal or greater version.
	Upsert(kind ldstoretypes.DataKind, key string, item ldstoretypes.ItemDescriptor) (bool, error)

	// IsStatusMonitoringEnabled returns true if this data store implementation supports status
	// monitoring.
	//
	// This is normally only true for persistent data stores created with ldcomponents.PersistentDataStore(),
	// but it could also be true for any custom DataStore implementation that makes use of the
	// statusUpdater parameter provided to the DataStoreFactory. Returning true means that the store
	// guarantees that if it ever enters an invalid state (that is, an operation has failed or it knows
	// that operations cannot succeed at the moment), it will publish a status update, and will then
	// publish another status update once it has returned to a valid state.
	//
	// The same value will be returned from DataStoreStatusProvider.IsStatusMonitoringEnabled().
	IsStatusMonitoringEnabled() bool
}
