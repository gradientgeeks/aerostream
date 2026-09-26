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
  { path: '**', redirectTo: 'cluster' },
];
