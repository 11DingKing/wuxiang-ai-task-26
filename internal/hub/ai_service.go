package hub

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

type AIServices struct {
	registry *AIRegistry
	now      func() time.Time
}

func NewAIServices(registry *AIRegistry, now func() time.Time) *AIServices {
	if registry == nil {
		registry = NewAIRegistry()
	}
	if now == nil {
		now = time.Now
	}
	return &AIServices{registry: registry, now: now}
}

func (s *AIServices) SubmitApplication(ctx context.Context, application EnterpriseApplication) (EnterpriseApplication, error) {
	if err := ctx.Err(); err != nil {
		return EnterpriseApplication{}, err
	}
	if err := validateApplicationInput(application); err != nil {
		return EnterpriseApplication{}, err
	}
	now := nowOr(s.now())
	application.CompanyName = strings.TrimSpace(application.CompanyName)
	application.ASEANCountry = normalizeCountry(application.ASEANCountry)
	application.Status = ApplicationPending
	application.Attempt = 1
	application.CreatedAt, application.UpdatedAt = now, now
	application.Materials = slices.Clone(application.Materials)
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if _, exists := s.registry.applications[application.ID]; exists {
		return EnterpriseApplication{}, fmt.Errorf("%w: application exists", ErrAIConflict)
	}
	s.registry.applications[application.ID] = application
	s.registry.events = append(s.registry.events, AIServiceEvent{ID: application.ID + "-submitted", TenantID: application.TenantID, EntityID: application.ID, Action: "application.submitted", ActorID: application.ApplicantID, At: now})
	return cloneApplication(application), nil
}

func cloneApplication(application EnterpriseApplication) EnterpriseApplication {
	application.Materials = slices.Clone(application.Materials)
	return application
}

func (s *AIServices) ReserveCompute(ctx context.Context, applicationID, clusterID string, petaops int) (ComputeReservation, error) {
	if err := ctx.Err(); err != nil {
		return ComputeReservation{}, err
	}
	if petaops <= 0 {
		return ComputeReservation{}, fmt.Errorf("%w: compute amount", ErrAIValidation)
	}
	now := nowOr(s.now())
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	application, ok := s.registry.applications[applicationID]
	if !ok {
		return ComputeReservation{}, ErrAIApplicationNotFound
	}
	cluster, ok := s.registry.clusters[clusterID]
	if !ok {
		return ComputeReservation{}, ErrAIClusterNotFound
	}
	if application.Status != ApplicationPending && application.Status != ApplicationReturned {
		return ComputeReservation{}, fmt.Errorf("%w: reservation requires pending application", ErrAIInvalidState)
	}
	if !computeAvailable(cluster, petaops) {
		return ComputeReservation{}, ErrAICapacity
	}
	if existing, exists := s.registry.reservations[applicationID]; exists && existing.ReleasedAt == nil {
		return existing, nil
	}
	cluster.Allocated += petaops
	cluster.Version++
	reservation := ComputeReservation{ApplicationID: applicationID, ClusterID: clusterID, Petaops: petaops, ReservedAt: now}
	s.registry.clusters[clusterID] = cluster
	s.registry.reservations[applicationID] = reservation
	application.UpdatedAt, application.Version = now, application.Version+1
	s.registry.applications[applicationID] = application
	return reservation, nil
}

func (s *AIServices) ReleaseCompute(ctx context.Context, applicationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := nowOr(s.now())
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	reservation, ok := s.registry.reservations[applicationID]
	if !ok || reservation.ReleasedAt != nil {
		return nil
	}
	cluster, ok := s.registry.clusters[reservation.ClusterID]
	if !ok {
		return ErrAIClusterNotFound
	}
	if cluster.Allocated < reservation.Petaops {
		return fmt.Errorf("%w: allocated compute underflow", ErrAIConflict)
	}
	cluster.Allocated -= reservation.Petaops
	cluster.Version++
	reservation.ReleasedAt = &now
	s.registry.clusters[cluster.ID] = cluster
	s.registry.reservations[applicationID] = reservation
	return nil
}

func (s *AIServices) ApproveApplication(ctx context.Context, applicationID, actorID string) (EnterpriseApplication, error) {
	if err := ctx.Err(); err != nil {
		return EnterpriseApplication{}, err
	}
	now := nowOr(s.now())
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	application, ok := s.registry.applications[applicationID]
	if !ok {
		return EnterpriseApplication{}, ErrAIApplicationNotFound
	}
	if actorID == "" {
		return EnterpriseApplication{}, ErrAIAuthorization
	}
	if !validApplicationTransition(application.Status, ApplicationApproved) {
		return EnterpriseApplication{}, fmt.Errorf("%w: approve from %s", ErrAIInvalidState, application.Status)
	}
	if _, ok := s.registry.reservations[applicationID]; !ok {
		return EnterpriseApplication{}, fmt.Errorf("%w: compute is not reserved", ErrAICapacity)
	}
	if application.RouteID != "" {
		route, exists := s.registry.routes[application.RouteID]
		if !exists || !route.Active {
			return EnterpriseApplication{}, fmt.Errorf("%w: route unavailable", ErrAIRouteNotFound)
		}
	}
	application.Status, application.UpdatedAt, application.Version = ApplicationApproved, now, application.Version+1
	s.registry.applications[applicationID] = application
	s.registry.events = append(s.registry.events, AIServiceEvent{ID: applicationID + "-approved", EntityID: applicationID, Action: "application.approved", ActorID: actorID, At: now})
	return cloneApplication(application), nil
}

func (s *AIServices) AssignRoute(ctx context.Context, applicationID, routeID string, mbps int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mbps <= 0 {
		return fmt.Errorf("%w: route capacity", ErrAIValidation)
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	application, ok := s.registry.applications[applicationID]
	if !ok {
		return ErrAIApplicationNotFound
	}
	route, ok := s.registry.routes[routeID]
	if !ok {
		return ErrAIRouteNotFound
	}
	if route.TenantID != application.TenantID || normalizeCountry(route.Country) != normalizeCountry(application.ASEANCountry) {
		return fmt.Errorf("%w: route tenant or country", ErrAIAuthorization)
	}
	if !route.Active || route.CapacityMbps-route.AllocatedMbps < mbps {
		return ErrAICapacity
	}
	route.AllocatedMbps += mbps
	route.Version++
	application.RouteID, application.Version = routeID, application.Version+1
	s.registry.routes[routeID], s.registry.applications[applicationID] = route, application
	return nil
}

func (s *AIServices) PublishScene(ctx context.Context, scene SceneSubmission) (SceneSubmission, error) {
	if err := ctx.Err(); err != nil {
		return SceneSubmission{}, err
	}
	if scene.ID == "" || scene.TenantID == "" || strings.TrimSpace(scene.OwnerID) == "" || scene.ApplicationID == "" {
		return SceneSubmission{}, fmt.Errorf("%w: scene identity", ErrAIValidation)
	}
	if len(scene.Evidence) < 2 {
		return SceneSubmission{}, fmt.Errorf("%w: at least two evidence items", ErrAIValidation)
	}
	scene.OwnerID = strings.TrimSpace(scene.OwnerID)
	now := nowOr(s.now())
	scene.Status, scene.SubmittedAt, scene.Evidence = "submitted", now, slices.Clone(scene.Evidence)
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	if _, exists := s.registry.scenes[scene.ID]; exists {
		return SceneSubmission{}, fmt.Errorf("%w: scene exists", ErrAIConflict)
	}
	if application, ok := s.registry.applications[scene.ApplicationID]; !ok || application.TenantID != scene.TenantID {
		return SceneSubmission{}, ErrAIApplicationNotFound
	}
	s.registry.scenes[scene.ID] = scene
	return cloneScene(scene), nil
}

func cloneScene(scene SceneSubmission) SceneSubmission {
	scene.Evidence = slices.Clone(scene.Evidence)
	return scene
}

func (s *AIServices) ApproveScene(ctx context.Context, sceneID, reviewerID string) (SceneSubmission, error) {
	if err := ctx.Err(); err != nil {
		return SceneSubmission{}, err
	}
	if reviewerID == "" {
		return SceneSubmission{}, ErrAIAuthorization
	}
	now := nowOr(s.now())
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	scene, ok := s.registry.scenes[sceneID]
	if !ok {
		return SceneSubmission{}, ErrAISceneNotFound
	}
	if err := sceneCanApprove(scene); err != nil {
		return SceneSubmission{}, err
	}
	scene.Status, scene.Version, scene.ReviewedAt = "approved", scene.Version+1, &now
	s.registry.scenes[sceneID] = scene
	return cloneScene(scene), nil
}

func (s *AIServices) ApplyOffer(ctx context.Context, applicationID, offerID string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	application, ok := s.registry.applications[applicationID]
	if !ok {
		return ErrAIApplicationNotFound
	}
	offer, ok := s.registry.offers[offerID]
	if !ok {
		return ErrAIOfferNotFound
	}
	if !offerAvailable(offer, application.ASEANCountry, nowOr(now)) {
		return ErrAICapacity
	}
	offer.Used++
	offer.Version++
	application.ServiceOfferID, application.Version = offerID, application.Version+1
	s.registry.offers[offerID], s.registry.applications[applicationID] = offer, application
	return nil
}

func (s *AIServices) EnrollOPC(ctx context.Context, communityID, memberID string) (OPCCommunity, error) {
	if err := ctx.Err(); err != nil {
		return OPCCommunity{}, err
	}
	if strings.TrimSpace(memberID) == "" {
		return OPCCommunity{}, fmt.Errorf("%w: member", ErrAIValidation)
	}
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	community, ok := s.registry.communities[communityID]
	if !ok {
		return OPCCommunity{}, ErrAIOPCNotFound
	}
	for _, existing := range community.MemberIDs {
		if existing == memberID {
			return cloneCommunity(community), nil
		}
	}
	if len(community.MemberIDs) >= community.Capacity {
		return OPCCommunity{}, ErrAICapacity
	}
	community.MemberIDs = append(community.MemberIDs, memberID)
	community.Version++
	s.registry.communities[communityID] = community
	return cloneCommunity(community), nil
}

func cloneCommunity(community OPCCommunity) OPCCommunity {
	community.MemberIDs = slices.Clone(community.MemberIDs)
	community.MentorIDs = slices.Clone(community.MentorIDs)
	community.Milestones = cloneStringMap(community.Milestones)
	return community
}

func (s *AIServices) ResubmitApplication(ctx context.Context, applicationID string, materials []string) (EnterpriseApplication, error) {
	if err := ctx.Err(); err != nil {
		return EnterpriseApplication{}, err
	}
	if len(materials) == 0 {
		return EnterpriseApplication{}, fmt.Errorf("%w: resubmission materials", ErrAIValidation)
	}
	now := nowOr(s.now())
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	application, ok := s.registry.applications[applicationID]
	if !ok {
		return EnterpriseApplication{}, ErrAIApplicationNotFound
	}
	if application.Status != ApplicationReturned {
		return EnterpriseApplication{}, fmt.Errorf("%w: resubmit from %s", ErrAIInvalidState, application.Status)
	}
	application.Status, application.Attempt, application.Materials, application.UpdatedAt, application.Version = ApplicationPending, application.Attempt+1, slices.Clone(materials), now, application.Version+1
	s.registry.applications[applicationID] = application
	return cloneApplication(application), nil
}

func (s *AIServices) CloseApplication(ctx context.Context, applicationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := nowOr(s.now())
	s.registry.mu.Lock()
	defer s.registry.mu.Unlock()
	application, ok := s.registry.applications[applicationID]
	if !ok {
		return ErrAIApplicationNotFound
	}
	if !validApplicationTransition(application.Status, ApplicationClosed) {
		return fmt.Errorf("%w: close from %s", ErrAIInvalidState, application.Status)
	}
	application.Status, application.UpdatedAt, application.Version = ApplicationClosed, now, application.Version+1
	s.registry.applications[applicationID] = application
	return nil
}

func (s *AIServices) ListApplications(ctx context.Context, query AIQuery) (AIQueryResult, error) {
	if err := ctx.Err(); err != nil {
		return AIQueryResult{}, err
	}
	if query.Offset < 0 || query.Limit < 0 || query.Limit > 100 {
		return AIQueryResult{}, fmt.Errorf("%w: pagination", ErrAIValidation)
	}
	s.registry.mu.RLock()
	defer s.registry.mu.RUnlock()
	items := make([]EnterpriseApplication, 0)
	for _, application := range s.registry.applications {
		if query.TenantID != "" && application.TenantID != query.TenantID {
			continue
		}
		if query.Status != "" && application.Status != query.Status {
			continue
		}
		if query.Country != "" && normalizeCountry(application.ASEANCountry) != normalizeCountry(query.Country) {
			continue
		}
		items = append(items, cloneApplication(application))
	}
	if query.Offset >= len(items) {
		return AIQueryResult{Applications: []EnterpriseApplication{}, Total: len(items)}, nil
	}
	end := len(items)
	if query.Limit > 0 && query.Offset+query.Limit < end {
		end = query.Offset + query.Limit
	}
	return AIQueryResult{Applications: slices.Clone(items[query.Offset:end]), Total: len(items)}, nil
}

// AI workflow boundary 26 keeps the public transition explicit for audit and replay.
func aiWorkflowBoundary26(value string) string {
	return value
}

func sharedMilestones(milestones map[string]string) map[string]string {
	if milestones == nil {
		return nil
	}
	shared := milestones
	return shared
}
