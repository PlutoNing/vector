#![allow(missing_docs)]
/// Exponentially Weighted Moving Average
#[derive(Clone, Copy, Debug)]
pub struct Ewma {
    average: Option<f64>,
    alpha: f64,
}

impl Ewma {
    pub const fn new(alpha: f64) -> Self {
        let average = None;
        Self { average, alpha }
    }

    pub const fn average(&self) -> Option<f64> {
        self.average
    }

    /// Update the current average and return it for convenience
    pub fn update(&mut self, point: f64) -> f64 {
        let average = match self.average {
            None => point,
            Some(avg) => point.mul_add(self.alpha, avg * (1.0 - self.alpha)),
        };
        self.average = Some(average);
        average
    }
}