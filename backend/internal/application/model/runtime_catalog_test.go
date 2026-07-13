package model

import (
	"context"
	"sync"
	"testing"

	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

type runtimeCatalogRepository struct {
	repository.ModelRepository
	mu        sync.Mutex
	revision  uint64
	routes    []modeldomain.Route
	listCalls int
}

func (r *runtimeCatalogRepository) RuntimeRevision(context.Context) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.revision, nil
}

func (r *runtimeCatalogRepository) ListEnabled(context.Context) ([]modeldomain.Route, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls++
	return cloneRoutes(r.routes), nil
}

func (r *runtimeCatalogRepository) replace(revision uint64, routes []modeldomain.Route) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revision = revision
	r.routes = cloneRoutes(routes)
}

func (r *runtimeCatalogRepository) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listCalls
}

func TestRuntimeCatalogReloadsOnlyAfterRevisionChanges(t *testing.T) {
	ctx := context.Background()
	repo := &runtimeCatalogRepository{
		revision: 1,
		routes:   []modeldomain.Route{{ID: 1, PublicID: "first", BoundAccountIDs: []uint64{7}}},
	}
	service := NewService(repo, nil, nil, nil)
	service.runtimeRevisionPollInterval = 0

	first, err := service.ListEnabled(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PublicID != "first" || repo.calls() != 1 {
		t.Fatalf("first snapshot = %#v, loads = %d", first, repo.calls())
	}
	first[0].PublicID = "mutated"
	first[0].BoundAccountIDs[0] = 99

	repo.replace(1, []modeldomain.Route{{ID: 2, PublicID: "not-visible"}})
	unchanged, err := service.ListEnabled(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(unchanged) != 1 || unchanged[0].PublicID != "first" || unchanged[0].BoundAccountIDs[0] != 7 || repo.calls() != 1 {
		t.Fatalf("unchanged snapshot = %#v, loads = %d", unchanged, repo.calls())
	}

	repo.replace(2, []modeldomain.Route{{ID: 2, PublicID: "second"}})
	changed, err := service.ListEnabled(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 1 || changed[0].PublicID != "second" || repo.calls() != 2 {
		t.Fatalf("changed snapshot = %#v, loads = %d", changed, repo.calls())
	}
	resolved, err := service.GetByPublicID(ctx, "second")
	if err != nil || resolved.ID != 2 || repo.calls() != 2 {
		t.Fatalf("resolved = %#v, err = %v, loads = %d", resolved, err, repo.calls())
	}
}

func TestRuntimeCatalogCoalescesConcurrentColdLoads(t *testing.T) {
	ctx := context.Background()
	repo := &runtimeCatalogRepository{revision: 1, routes: []modeldomain.Route{{ID: 1, PublicID: "shared"}}}
	service := NewService(repo, nil, nil, nil)
	service.runtimeRevisionPollInterval = 0

	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			values, err := service.ListEnabled(ctx)
			if err != nil || len(values) != 1 || values[0].PublicID != "shared" {
				t.Errorf("values = %#v, err = %v", values, err)
			}
		}()
	}
	group.Wait()
	if repo.calls() != 1 {
		t.Fatalf("cold catalog loads = %d, want 1", repo.calls())
	}
}
