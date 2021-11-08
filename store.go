package store

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go/v4"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

var (
	// ErrNotFound is used when a specific Product is requested but does not exist.
	ErrNotFound = errors.New("document not found")

	// ErrInvalidID occurs when an ID is not in a valid form.
	ErrInvalidID = errors.New("ID is not in its proper form")

	// ErrDocsNotFound occurs when a documents could not be found on firbase
	ErrDocsNotFound = errors.New("error getting documents snapshots")

	// ErrForbidden occurs when a user tries to do something that is forbidden to them according to our access control policies.
	ErrForbidden = errors.New("attempted action is not allowed")

	ctx = context.Background()
)

// Firestore is a firebase firestore client
type Firestore struct {
	Client *firestore.Client
}

// New return a *Firestore{} instance
func New(opt option.ClientOption) (*Firestore, error) {

	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		return nil, errors.New("error initializing app")
	}

	fc, err := app.Firestore(ctx)
	if err != nil {
		return nil, errors.New("error getting firestoreClient")
	}

	store := Firestore{
		Client: fc,
	}

	return &store, nil
}

// FindOneByField returns a document by field
func (fs *Firestore) FindOneByField(ctx context.Context, c, f, op string, v interface{}) (*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Where(f, op, v).Limit(1).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds[0], nil
}

// FindOneByTwoFields returns a document by field
func (fs *Firestore) FindOneByTwoFields(ctx context.Context, c, ff, fop string, fv interface{}, sf, sop string, sv interface{}) (*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Where(ff, fop, fv).Where(sf, sop, sv).Limit(1).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds[0], nil
}

// Delete removes a document by id
func (fs *Firestore) Delete(ctx context.Context, ref *firestore.DocumentRef) error {

	if _, err := ref.Delete(ctx); err != nil {
		return err
	}

	return nil
}

// FindAllByField returns a document by field
func (fs *Firestore) FindAllByField(ctx context.Context, c, f, op string, v interface{}) ([]*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Where(f, op, v).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds, nil
}

// FindAllByFieldAndOrder returns all documents by field in order
func (fs *Firestore) FindAllByFieldAndOrder(ctx context.Context, c, f, op string, v interface{}, p string, dir firestore.Direction) ([]*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Where(f, op, v).OrderBy(p, dir).Documents(ctx).GetAll()
	if err != nil {
		fmt.Println(err)
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds, nil
}

// FindAllByTwoFields returns a document by field
func (fs *Firestore) FindAllByTwoFields(ctx context.Context, c string, ff string, fop string, fv interface{}, sf string, sop string, sv interface{}) ([]*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Where(ff, fop, fv).Where(sf, sop, sv).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds, nil
}

// FindFromArray returns a documents by field
func (fs *Firestore) FindFromArray(ctx context.Context, c, f, v string) ([]*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Where(f, "array-contains", v).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds, nil
}

// GetAll returns all documents in a colloctions
func (fs *Firestore) GetAll(ctx context.Context, c string) ([]*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds, nil
}

// GetAllByOrder returns all documents in a colloctions
func (fs *Firestore) GetAllByOrder(ctx context.Context, c, p string, dir firestore.Direction) ([]*firestore.DocumentSnapshot, error) {

	ds, err := fs.Client.Collection(c).OrderBy(p, dir).Documents(ctx).GetAll()
	if err != nil {
		return nil, ErrDocsNotFound
	}

	if len(ds) <= 0 {
		return nil, ErrNotFound
	}

	return ds, nil
}

// Add adds a new document to a collection
func (fs *Firestore) Add(ctx context.Context, c string, d interface{}) (*firestore.DocumentSnapshot, error) {

	dRef, _, err := fs.Client.Collection(c).Add(ctx, d)
	if err != nil {
		return nil, errors.Wrap(err, "adding document")
	}

	ds, err := dRef.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "getting document snapshot from firebase")
	}

	return ds, nil
}

// Update updates a document ref and return updated ref
func (fs *Firestore) Update(ctx context.Context, df *firestore.DocumentRef, data interface{}) error {

	if _, err := df.Set(ctx, data); err != nil {
		return errors.Wrapf(err, "updating document %s")
	}

	return nil
}
