import { Mutex } from "https://deno.land/x/async@v2.1.0/mod.ts";

export class RWMutex {
  private write: Mutex;
  private read: Mutex;

  constructor() {
    this.write = new Mutex();
    this.read = new Mutex();
  }

  async acquireWrite() {
    await this.write.acquire();
    try {
      await this.read.acquire();
    } catch (e) {
      this.write.release();
      throw e;
    }

    return () => {
      this.read.release();
      this.write.release();
    };
  }

  async acquireRead() {
    const writeLocked = this.write.locked;
    if (writeLocked) {
      await this.write.acquire();
    }

    // acquire read lock without blocking
    this.read.acquire();

    return () => {
      this.read.release();
      if (writeLocked) {
        this.write.release();
      }
    };
  }
}
