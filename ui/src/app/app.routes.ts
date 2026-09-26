import { Routes } from '@angular/router';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'cluster' },
  {
    path: 'cluster',
    loadComponent: () =>
      import('./components/cluster-overview/cluster-overview.component').then(
        (m) => m.ClusterOverviewComponent
      ),
  },
  {
    path: 'topics',
    loadComponent: () =>
      import('./components/topics/topics.component').then(
        (m) => m.TopicsComponent
      ),
  },
  {
    path: 'messages',
    loadComponent: () =>
      import('./components/messages/messages.component').then(
        (m) => m.MessagesComponent
      ),
  },
  {
    path: 'groups',
    loadComponent: () =>
      import('./components/consumer-groups/consumer-groups.component').then(
        (m) => m.ConsumerGroupsComponent
      ),
  },
  {
    path: 'producer',
    loadComponent: () =>
      import('./components/producer/producer.component').then(
        (m) => m.ProducerComponent
      ),
  },
  {
    path: 'schemas',
    loadComponent: () =>
      import('./components/schema-registry/schema-registry.component').then(
        (m) => m.SchemaRegistryComponent
      ),
  },
  {
    path: 'acls',
    loadComponent: () =>
      import('./components/acls/acls.component').then(
        (m) => m.AclsComponent
      ),
  },
  {
    path: 'connectors',
    loadComponent: () =>
      import('./components/connectors/connectors.component').then(
        (m) => m.ConnectorsComponent
      ),
  },
  {
    path: 'transforms',
    loadComponent: () =>
      import('./components/transforms/transforms.component').then(
        (m) => m.TransformsComponent
      ),
  },
  {
    path: 'aerostream/console/transforms',
    redirectTo: 'transforms',
  },
  {
    path: 'aerostream/console/connectors',
    redirectTo: 'connectors',
  },
  {
    path: 'aerostream/console/acls',
    redirectTo: 'acls',
  },
  {
    path: 'aerostream/console/schemas',
    redirectTo: 'schemas',
  },
  { path: '**', redirectTo: 'cluster' },
];
