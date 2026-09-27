import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    loadComponent: () =>
      import('./components/showcase/showcase.component').then(
        (m) => m.ShowcaseComponent
      ),
    title: 'AeroStream | Next-Gen Dual-Engine Distributed Streaming Platform',
  },
  {
    path: 'showcase',
    redirectTo: '',
    pathMatch: 'full',
  },
  {
    path: 'docs',
    loadComponent: () =>
      import('./components/docs/docs.component').then((m) => m.DocsComponent),
    title: 'AeroStream Documentation | Architecture, Kafka Protocol & Guide',
  },
  {
    path: '**',
    redirectTo: '',
  },
];
