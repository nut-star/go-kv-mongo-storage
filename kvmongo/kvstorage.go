package kvmongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type KVStorage[K comparable, V any] interface {
	Get(key K) (V, KVError)
	Put(key K, value V) (bool, KVError)
	PutF(key K, valueF func(interface{}) V) (bool, KVError)
	Remove(key K) (bool, KVError)
}

type KVCollection[K comparable, V any] struct {
	ctx        context.Context
	collection *mongo.Collection
	retryCount int
}

func NewKVStorage[K comparable, V any](ctx context.Context, collection *mongo.Collection) *KVCollection[K, V] {
	return &KVCollection[K, V]{ctx, collection, 3}
}

func (c *KVCollection[K, V]) Get(key K) (V, KVError) {
	var i item[K, V]
	var err KVError
	res := c.collection.FindOne(c.ctx, bson.M{"_id": key})
	if res.Err() != nil {
		return i.value, parseKVError(res.Err())
	}
	errD := res.Decode(&i)
	if errD != nil {
		if errD == mongo.ErrNoDocuments {
			err = newKVError(NotFoundErr, errD)
		} else {
			err = newKVError(DecodeErr, errD)
		}
	}

	return i.value, err
}

func (c *KVCollection[K, V]) Remove(key K) (bool, KVError) {
	_, err := c.collection.DeleteOne(c.ctx, bson.M{"_id": key})
	if err != nil {
		return false, parseKVError(err)
	}
	return true, nil
}

func (c *KVCollection[K, V]) Put(key K, value V) (bool, KVError) {
	updateF := func(interface{}) V {
		return value
	}
	return c.PutF(key, updateF)
}

func (c *KVCollection[K, V]) PutF(key K, valueF func(interface{}) V) (bool, KVError) {
	var old item[K, V]

	_, errI := c.collection.InsertOne(c.ctx, item[K, V]{key, 1, valueF(nil)})
	if errI == nil {
		return true, nil
	}
	count := 0
	result := false
	var errResult KVError
	for count < c.retryCount {
		errF := c.collection.FindOne(c.ctx, bson.M{"_id": key}).Decode(&old)
		if errF != nil {
			return false, parseKVError(errF)
		}

		opts := options.Update().SetUpsert(true)
		filter := bson.M{
			"_id":     key,
			"version": old.version,
		}
		update := bson.D{{Key: "$set", Value: bson.M{
			"value":   valueF(old.value),
			"version": old.version + 1,
		}}}

		_, errU := c.collection.UpdateOne(c.ctx, filter, update, opts)
		if errU != nil {
			errResult = parseKVError(errU)
			result = false
			if errResult.Code() == OptimisticLockErr {
				count++
			} else {
				return false, errResult
			}
		} else {
			return true, nil
		}
	}
	return result, errResult
}
