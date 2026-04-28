use lib::{RmsProcessor, PitchProcessor};

mod lib;

pub fn main() {
    let rms_proc = RmsProcessor::new(20.0, 20000.0);
    let pitch_proc = PitchProcessor::new(None);

    loop {
        if let Some(rms_data) = RmsProcessor::drain(&rms_proc) {
            println!("RMS {:?}", rms_data);
        }

        if let Some(pitch_data) = PitchProcessor::drain(&pitch_proc) {
            println!("PITCH {:?}", pitch_data);    
        }
    }
}