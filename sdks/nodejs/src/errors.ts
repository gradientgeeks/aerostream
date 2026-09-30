export class AeroError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AeroError';
  }
}

export class AuthenticationError extends AeroError {
  constructor(message = 'Authentication failed with AeroStream broker') {
    super(message);
    this.name = 'AuthenticationError';
  }
}

export class OutOfOrderError extends AeroError {
  constructor(message = 'Out of order sequence number rejected by broker') {
    super(message);
    this.name = 'OutOfOrderError';
  }
}

export class ConnectionError extends AeroError {
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'ConnectionError';
  }
}

export class TimeoutError extends AeroError {
  constructor(message = 'Operation timed out') {
    super(message);
    this.name = 'TimeoutError';
  }
}
