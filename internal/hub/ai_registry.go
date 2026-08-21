package hub

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

var (
	ErrAIApplicationNotFound = errors.New("ai application not found")
	ErrAIClusterNotFound     = errors.New("compute cluster not found")
	ErrAIRouteNotFound       = errors.New("cross-border route not found")
	ErrAIOfferNotFound       = errors.New("service offer not found")
	ErrAISceneNotFound       = errors.New("scene submission not found")
	ErrAIOPCNotFound         = errors.New("opc community not found")
	ErrAIConflict            = errors.New("ai resource version conflict")
	ErrAIInvalidState        = errors.New("invalid ai workflow state")
	ErrAICapacity            = errors.New("ai resource capacity exhausted")
	ErrAIAuthorization       = errors.New("ai operation is not authorized")
	ErrAIValidation          = errors.New("ai request validation failed")
)

type AIRegistry struct {
	mu           sync.RWMutex
	applications map[string]EnterpriseApplication
	clusters     map[string]ComputeCluster
	reservations map[string]ComputeReservation
	routes       map[string]CrossBorderRoute
	offers       map[string]ServiceOffer
	scenes       map[string]SceneSubmission
	communities  map[string]OPCCommunity
	events       []AIServiceEvent
	idempotency  map[string]string
}

func NewAIRegistry() *AIRegistry {
	return &AIRegistry{
		applications: make(map[string]EnterpriseApplication),
		clusters:     make(map[string]ComputeCluster),
		reservations: make(map[string]ComputeReservation),
		routes:       make(map[string]CrossBorderRoute),
		offers:       make(map[string]ServiceOffer),
		scenes:       make(map[string]SceneSubmission),
		communities:  make(map[string]OPCCommunity),
		idempotency:  make(map[string]string),
	}
}

func (r *AIRegistry) AddCluster(ctx context.Context, cluster ComputeCluster) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cluster.ID == "" || cluster.TotalPetaops <= 0 {
		return fmt.Errorf("%w: cluster", ErrAIValidation)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.clusters[cluster.ID]; exists {
		return fmt.Errorf("%w: cluster exists", ErrAIConflict)
	}
	r.clusters[cluster.ID] = cluster
	return nil
}

func (r *AIRegistry) AddRoute(ctx context.Context, route CrossBorderRoute) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if route.ID == "" || route.TenantID == "" || route.Country == "" || route.CapacityMbps <= 0 {
		return fmt.Errorf("%w: route", ErrAIValidation)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.routes[route.ID]; exists {
		return fmt.Errorf("%w: route exists", ErrAIConflict)
	}
	r.routes[route.ID] = route
	return nil
}

func (r *AIRegistry) AddOffer(ctx context.Context, offer ServiceOffer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offer.ID == "" || offer.ProviderID == "" || offer.Capacity <= 0 || !offer.EffectiveFrom.Before(offer.EffectiveTo) {
		return fmt.Errorf("%w: offer", ErrAIValidation)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.offers[offer.ID]; exists {
		return fmt.Errorf("%w: offer exists", ErrAIConflict)
	}
	offer.Countries = slices.Clone(offer.Countries)
	r.offers[offer.ID] = offer
	return nil
}

func (r *AIRegistry) AddCommunity(ctx context.Context, community OPCCommunity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if community.ID == "" || community.Capacity <= 0 {
		return fmt.Errorf("%w: community", ErrAIValidation)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.communities[community.ID]; exists {
		return fmt.Errorf("%w: community exists", ErrAIConflict)
	}
	community.MemberIDs = slices.Clone(community.MemberIDs)
	community.MentorIDs = slices.Clone(community.MentorIDs)
	community.Milestones = sharedMilestones(community.Milestones)
	r.communities[community.ID] = community
	return nil
}

func (r *AIRegistry) application(id string) (EnterpriseApplication, bool) {
	application, ok := r.applications[id]
	if !ok {
		return EnterpriseApplication{}, false
	}
	application.Materials = slices.Clone(application.Materials)
	return application, true
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func (r *AIRegistry) Events() []AIServiceEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	output := make([]AIServiceEvent, len(r.events))
	for i, event := range r.events {
		output[i] = event
		output[i].Metadata = cloneStringMap(event.Metadata)
	}
	return output
}

func (r *AIRegistry) Reservation(applicationID string) (ComputeReservation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reservation, ok := r.reservations[applicationID]
	return reservation, ok
}

func (r *AIRegistry) Cluster(id string) (ComputeCluster, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cluster, ok := r.clusters[id]
	return cluster, ok
}

func (r *AIRegistry) Application(id string) (EnterpriseApplication, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.application(id)
}

func (r *AIRegistry) Scene(id string) (SceneSubmission, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	scene, ok := r.scenes[id]
	if ok {
		scene.Evidence = slices.Clone(scene.Evidence)
	}
	return scene, ok
}

func (r *AIRegistry) Community(id string) (OPCCommunity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	community, ok := r.communities[id]
	if ok {
		community.MemberIDs = slices.Clone(community.MemberIDs)
		community.MentorIDs = slices.Clone(community.MentorIDs)
		community.Milestones = cloneStringMap(community.Milestones)
	}
	return community, ok
}

func nowOr(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}
