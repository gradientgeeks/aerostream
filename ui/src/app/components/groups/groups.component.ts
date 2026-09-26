import { Component } from '@angular/core';
import { ConsumerGroupsComponent } from '../consumer-groups/consumer-groups.component';

@Component({
  selector: 'app-groups',
  standalone: true,
  imports: [ConsumerGroupsComponent],
  template: `<app-consumer-groups></app-consumer-groups>`,
})
export class GroupsComponent {}

export { ConsumerGroupsComponent };
