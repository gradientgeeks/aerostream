import { Component, Input, ChangeDetectionStrategy } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-turbine-logo',
  standalone: true,
  imports: [CommonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div
      class="turbine-container"
      [class.is-animated]="animated"
      [class.has-glow]="glow"
      [style.width.px]="size"
      [style.height.px]="size"
      role="img"
      aria-label="AeroStream Turbine Ring Buffer Logo">
      <svg
        [attr.width]="size"
        [attr.height]="size"
        viewBox="0 0 100 100"
        xmlns="http://www.w3.org/2000/svg"
        class="turbine-svg">
        <defs>
          <!-- Dual Go Gradient (#0284C7 -> #00E5FF -> #FFFFFF) -->
          <linearGradient [id]="'goGrad_' + uid" x1="0%" y1="100%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#0284C7" />
            <stop offset="60%" stop-color="#00E5FF" />
            <stop offset="100%" stop-color="#FFFFFF" />
          </linearGradient>

          <!-- Dual Rust Gradient (#FF5722 -> #FFB300 -> #FFFFFF) -->
          <linearGradient [id]="'rustGrad_' + uid" x1="0%" y1="100%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#FF5722" />
            <stop offset="65%" stop-color="#FFB300" />
            <stop offset="100%" stop-color="#FFFFFF" />
          </linearGradient>

          <!-- Speed Trail Flow Gradients -->
          <linearGradient [id]="'goTrail_' + uid" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stop-color="#00E5FF" stop-opacity="0" />
            <stop offset="70%" stop-color="#00E5FF" stop-opacity="0.6" />
            <stop offset="100%" stop-color="#00E5FF" stop-opacity="1" />
          </linearGradient>

          <linearGradient [id]="'rustTrail_' + uid" x1="100%" y1="100%" x2="0%" y2="0%">
            <stop offset="0%" stop-color="#FF5722" stop-opacity="0" />
            <stop offset="70%" stop-color="#FFB300" stop-opacity="0.6" />
            <stop offset="100%" stop-color="#FF5722" stop-opacity="1" />
          </linearGradient>

          <!-- Inner Hub Metallic Gradient -->
          <radialGradient [id]="'hubGrad_' + uid" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stop-color="#1e293b" />
            <stop offset="65%" stop-color="#0f172a" />
            <stop offset="100%" stop-color="#020617" />
          </radialGradient>

          <!-- Raft Quorum Leader Glow -->
          <radialGradient [id]="'leaderGrad_' + uid" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stop-color="#FFFFFF" />
            <stop offset="35%" stop-color="#00E5FF" />
            <stop offset="100%" stop-color="#0284C7" />
          </radialGradient>

          <!-- Glow Filter -->
          <filter [id]="'glowFilter_' + uid" x="-25%" y="-25%" width="150%" height="150%">
            <feGaussianBlur stdDeviation="1.2" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>

          <!-- Swept-Back Turbine Airfoil Vane Master Template -->
          <g [id]="'vanePath_' + uid">
            <path d="M 48.6 32 C 51.5 24.2 61.2 16.5 76.5 19.8 C 77.4 20.5 77.2 21.5 76.2 22.3 C 63.8 26.6 57.2 30.2 54.4 32.7 A 18.2 18.2 0 0 0 48.6 32 Z" />
            <path d="M 49.6 31.2 C 52.4 24.6 61.6 18.2 75.2 20.4" fill="none" stroke="rgba(255, 255, 255, 0.55)" stroke-width="0.65" stroke-linecap="round" />
          </g>
        </defs>

        <!-- 1. Speed Trails in Queue Orbit (Outer) -->
        <g class="speed-trails-group">
          <!-- Queue Orbit Guide Tracks -->
          <circle cx="50" cy="50" r="46.5" fill="none" stroke="rgba(255, 255, 255, 0.08)" stroke-width="0.6" />
          <circle cx="50" cy="50" r="46.5" fill="none" stroke="rgba(0, 229, 255, 0.2)" stroke-width="0.8" stroke-dasharray="2 8" />

          <!-- Go Speed Trail Arc -->
          <path
            d="M 50 3.5 A 46.5 46.5 0 0 1 96.5 50"
            fill="none"
            [attr.stroke]="'url(#goTrail_' + uid + ')'"
            stroke-width="1.8"
            stroke-linecap="round"
            [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null" />
          <circle cx="96.5" cy="50" r="1.8" fill="#00E5FF" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null" />

          <!-- Rust Speed Trail Arc -->
          <path
            d="M 50 96.5 A 46.5 46.5 0 0 1 3.5 50"
            fill="none"
            [attr.stroke]="'url(#rustTrail_' + uid + ')'"
            stroke-width="1.8"
            stroke-linecap="round"
            [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null" />
          <circle cx="3.5" cy="50" r="1.8" fill="#FF5722" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null" />

          <!-- Orbiting Queue Packet Sparks -->
          <circle cx="82.8" cy="17.2" r="1.2" fill="#FFFFFF" opacity="0.9" />
          <circle cx="17.2" cy="82.8" r="1.2" fill="#FFB300" opacity="0.9" />
        </g>

        <!-- 2. Outer Shroud Ring -->
        <circle cx="50" cy="50" r="43.5" fill="none" stroke="rgba(255, 255, 255, 0.15)" stroke-width="0.8" />
        <circle cx="50" cy="50" r="42.5" fill="none" stroke="rgba(0, 229, 255, 0.25)" stroke-width="0.5" stroke-dasharray="1 3" />

        <!-- 3. 6 Swept-Back Turbine Airfoil Vanes (Dual Go / Rust Engines) -->
        <g class="rotor-blades-group">
          <!-- Vane 0: Go Engine -->
          <g transform="rotate(0 50 50)" [attr.fill]="'url(#goGrad_' + uid + ')'" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null">
            <use [attr.href]="'#vanePath_' + uid" />
          </g>
          <!-- Vane 1: Rust Engine -->
          <g transform="rotate(60 50 50)" [attr.fill]="'url(#rustGrad_' + uid + ')'" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null">
            <use [attr.href]="'#vanePath_' + uid" />
          </g>
          <!-- Vane 2: Go Engine -->
          <g transform="rotate(120 50 50)" [attr.fill]="'url(#goGrad_' + uid + ')'" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null">
            <use [attr.href]="'#vanePath_' + uid" />
          </g>
          <!-- Vane 3: Rust Engine -->
          <g transform="rotate(180 50 50)" [attr.fill]="'url(#rustGrad_' + uid + ')'" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null">
            <use [attr.href]="'#vanePath_' + uid" />
          </g>
          <!-- Vane 4: Go Engine -->
          <g transform="rotate(240 50 50)" [attr.fill]="'url(#goGrad_' + uid + ')'" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null">
            <use [attr.href]="'#vanePath_' + uid" />
          </g>
          <!-- Vane 5: Rust Engine -->
          <g transform="rotate(300 50 50)" [attr.fill]="'url(#rustGrad_' + uid + ')'" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null">
            <use [attr.href]="'#vanePath_' + uid" />
          </g>
        </g>

        <!-- 4. Inner mmap Ring Buffer Hub -->
        <g class="hub-group">
          <!-- Hub Base Plate -->
          <circle cx="50" cy="50" r="18.5" [attr.fill]="'url(#hubGrad_' + uid + ')'" stroke="rgba(255, 255, 255, 0.2)" stroke-width="0.7" />
          <circle cx="50" cy="50" r="17.2" fill="none" stroke="#00E5FF" stroke-width="0.8" opacity="0.5" />

          <!-- mmap Memory Ring Buffer Track -->
          <circle class="mmap-track" cx="50" cy="50" r="15.5" fill="none" stroke="#00E5FF" stroke-width="0.9" stroke-dasharray="2.2 1.8" opacity="0.85" />
          <circle cx="50" cy="50" r="13.2" fill="none" stroke="rgba(255, 87, 34, 0.45)" stroke-width="0.6" stroke-dasharray="1 2.5" />

          <!-- Ring Buffer Read / Write Pointers -->
          <circle cx="65" cy="50" r="1" fill="#00E5FF" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null" />
          <circle cx="35" cy="50" r="1" fill="#FF5722" [attr.filter]="glow ? 'url(#glowFilter_' + uid + ')' : null" />

          <!-- Hub Core Center Plate -->
          <circle cx="50" cy="50" r="12" fill="#070d19" stroke="rgba(255, 255, 255, 0.1)" stroke-width="0.5" />
        </g>

        <!-- 5. Raft Consensus Quorum Triangle (Apex Leader + 2 Followers) -->
        <g class="raft-quorum-group">
          <!-- Quorum Region Fill -->
          <polygon points="50,42 42.5,54.5 57.5,54.5" fill="rgba(0, 229, 255, 0.08)" />

          <!-- Consensus RPC Communication Links -->
          <line class="raft-link" x1="50" y1="42" x2="42.5" y2="54.5" stroke="#00E5FF" stroke-width="0.85" />
          <line class="raft-link" x1="50" y1="42" x2="57.5" y2="54.5" stroke="#00E5FF" stroke-width="0.85" />
          <line class="raft-link" x1="42.5" y1="54.5" x2="57.5" y2="54.5" stroke="#FFB300" stroke-width="0.85" />

          <!-- Quorum Center Beacon -->
          <circle cx="50" cy="50.3" r="0.8" fill="#FFFFFF" opacity="0.8" />

          <!-- Follower Node 1 (Left) -->
          <circle cx="42.5" cy="54.5" r="1.8" fill="#0284C7" stroke="#00E5FF" stroke-width="0.6" />
          <circle cx="42.5" cy="54.5" r="0.7" fill="#FFFFFF" />

          <!-- Follower Node 2 (Right) -->
          <circle cx="57.5" cy="54.5" r="1.8" fill="#FF5722" stroke="#FFB300" stroke-width="0.6" />
          <circle cx="57.5" cy="54.5" r="0.7" fill="#FFFFFF" />

          <!-- Apex Leader Node (Top) with Halo and Crown -->
          <g class="raft-leader">
            <circle cx="50" cy="42" r="3" fill="none" stroke="#00E5FF" stroke-width="0.5" opacity="0.6" />
            <circle cx="50" cy="42" r="2.2" [attr.fill]="'url(#leaderGrad_' + uid + ')'" stroke="#FFFFFF" stroke-width="0.7" />
            <circle cx="50" cy="42" r="0.8" fill="#FFFFFF" />
          </g>
        </g>
      </svg>
    </div>
  `,
  styles: [`
    :host {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      vertical-align: middle;
      line-height: 0;
    }

    .turbine-container {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      position: relative;
    }

    .turbine-svg {
      width: 100%;
      height: 100%;
      overflow: visible;
      display: block;
    }

    /* Crisp static logo presentation */
    .turbine-svg {
      filter: none;
    }
  `]
})
export class TurbineLogoComponent {
  @Input() size = 36;
  @Input() animated = false;
  @Input() glow = false;

  protected readonly uid = Math.random().toString(36).substring(2, 8);
}
