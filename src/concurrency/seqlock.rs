//! Seqlock for single-writer, lock-free multi-reader state and `LatestCache<T>`.

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering, fence};

/// Lock-free single-writer, multi-reader seqlock protecting a `Copy` value.
#[repr(C)]
pub struct SeqLockSlot<T: Copy> {
    seq: AtomicU64,
    value: UnsafeCell<MaybeUninit<T>>,
    has_value: AtomicU8,
}

unsafe impl<T: Copy + Send> Send for SeqLockSlot<T> {}
unsafe impl<T: Copy + Send + Sync> Sync for SeqLockSlot<T> {}

impl<T: Copy> SeqLockSlot<T> {
    pub const fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            value: UnsafeCell::new(MaybeUninit::uninit()),
            has_value: AtomicU8::new(0),
        }
    }

    #[inline(always)]
    pub fn write(&self, val: T) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Release);
        fence(Ordering::AcqRel);
        unsafe {
            ptr::write((*self.value.get()).as_mut_ptr(), val);
        }
        self.has_value.store(1, Ordering::Release);
        self.seq.store(s.wrapping_add(2), Ordering::Release);
    }

    #[inline(always)]
    pub fn invalidate(&self) {
        self.has_value.store(0, Ordering::Release);
    }

    #[inline(always)]
    pub fn version(&self) -> u64 {
        self.seq.load(Ordering::Acquire) >> 1
    }

    #[inline(always)]
    pub fn try_read(&self) -> Option<(T, u64)> {
        if self.has_value.load(Ordering::Acquire) == 0 {
            return None;
        }
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                core::hint::spin_loop();
                continue;
            }
            // Volatile read prevents tearing / reordering across sequence checks.
            let val = unsafe { ptr::read_volatile((*self.value.get()).as_ptr()) };
            fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                if self.has_value.load(Ordering::Acquire) == 0 {
                    return None;
                }
                return Some((val, s1 >> 1));
            }
        }
    }
}

/// Low-level seqlock helpers over an `AtomicI64` sequence counter (used by `OrderBook`).
#[inline(always)]
pub fn begin_write(seq: &AtomicI64) {
    let s = seq.load(Ordering::Relaxed);
    seq.store(s + 1, Ordering::Release);
    fence(Ordering::SeqCst);
}

#[inline(always)]
pub fn end_write(seq: &AtomicI64) {
    let s = seq.load(Ordering::Relaxed);
    seq.store(s + 1, Ordering::Release);
}

#[inline(always)]
pub fn begin_read(seq: &AtomicI64) -> i64 {
    loop {
        let s = seq.load(Ordering::Acquire);
        if (s & 1) == 0 {
            return s;
        }
        core::hint::spin_loop();
    }
}

#[inline(always)]
pub fn end_read(seq: &AtomicI64, start: i64) -> bool {
    fence(Ordering::SeqCst);
    seq.load(Ordering::Acquire) == start
}

/// Lock-free cache holding the latest value per subscription handle.
pub struct LatestCache<T: Copy> {
    slots: Box<[SeqLockSlot<T>]>,
}

impl<T: Copy> LatestCache<T> {
    pub fn new(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(SeqLockSlot::new());
        }
        Self {
            slots: vec.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn update(&self, subscription: usize, value: T) {
        if let Some(slot) = self.slots.get(subscription) {
            slot.write(value);
        }
    }

    #[inline(always)]
    pub fn disconnect(&self, subscription: usize) {
        if let Some(slot) = self.slots.get(subscription) {
            slot.invalidate();
        }
    }

    #[inline(always)]
    pub fn try_read(&self, subscription: usize) -> Option<(T, u64)> {
        self.slots.get(subscription)?.try_read()
    }
}

