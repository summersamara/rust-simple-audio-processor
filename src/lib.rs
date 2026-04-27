
use serde::{Deserialize, Serialize};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use spectrum_analyzer::scaling::divide_by_N_sqrt;
use spectrum_analyzer::windows::hann_window;
use spectrum_analyzer::{samples_fft_to_spectrum, Frequency, FrequencyLimit, FrequencyValue};
use aubio_rs::Pitch;
use aubio_rs::PitchMode;
use std::sync::mpsc::{self, Receiver};
use std::thread;

const CHUNK_SIZE: usize = 4096;
const SAMPLING_RATE: u32 = 44100;

struct AudioStream {
    receiver: Receiver<Vec<f32>>,
}

impl AudioStream {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();

        let host = cpal::default_host();
        let device = host.default_input_device().expect("no input device available");
        let supported_config = device.supported_input_configs()
            .expect("error while querying configs")
            .next()
            .expect("no supported config?!")
            .with_max_sample_rate();

        thread::spawn(move || {
            let mut samples: Vec<f32> = vec![];
            let stream = device.build_input_stream(
                &supported_config.clone().into(),
                move |data: &[f32], _: &cpal::InputCallbackInfo| {
                    samples.extend(data);
                    while samples.len() >= CHUNK_SIZE {
                        let chunk = samples.drain(..CHUNK_SIZE).collect();
                        tx.send(chunk).unwrap();
                    }
                },
                move |err| {
                    eprintln!("Error: {0}", err);
                },
                None,
            ).unwrap();

            stream.play().expect("couldn't start input stream");
            loop {}
        });

        Self {
            receiver: rx
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Default, Debug)]
pub struct RmsData {
    rms: f32,
    last_rms: f32,
    rms_peak: f32,
}

pub struct RmsProcessor {
    rx: Receiver<RmsData>
}

impl RmsProcessor {
    pub fn new(frequency_limit: FrequencyLimit) -> Self {
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut last_data = RmsData::default();
            let stream = AudioStream::new();

            loop {
                let samples = stream.receiver.recv().unwrap();
                let hann_window = hann_window(&samples);

                let spectrum = samples_fft_to_spectrum(
                    &hann_window,
                    SAMPLING_RATE,
                    frequency_limit,
                    Some(&divide_by_N_sqrt),
                ).expect("Failed to compute FFT spectrum");

                let rms_data = RmsProcessor::calculate(spectrum.data(), frequency_limit, &last_data);

                tx.send(rms_data).unwrap();
                last_data = rms_data;
            }
        });

        Self {
            rx
        }
    }

    fn calculate(
        frequency_spec_data: &[(Frequency, FrequencyValue)],
        frequency_limit: FrequencyLimit,
        last_data: &RmsData,
    ) -> RmsData {
        let squared_values: Vec<f32> = frequency_spec_data
            .iter()
            .filter(|(freq, _)| freq.val() > frequency_limit.min() && freq.val() < frequency_limit.max())
            .map(|(_, value)| value.val().powi(2))
            .collect();
        
        let squared_sum: f32 = squared_values.iter().sum();
        let squared_avg = squared_sum / squared_values.len() as f32;
        let rms = squared_avg.sqrt();
        let is_peak = rms > last_data.rms_peak;

        RmsData {
            rms,
            last_rms: last_data.rms,
            rms_peak: if is_peak { rms } else { last_data.rms_peak },
        }
    }

    pub fn drain(proc: &RmsProcessor) -> Option<RmsData> {
        let mut latest: Option<RmsData> = None;
        
        while let Ok(rms_data) = proc.rx.try_recv() {
            latest = Some(rms_data);
        }

        latest
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Default, Debug)]
pub struct PitchData {
    pitch: f32,
    last_pitch: f32,
}

pub struct PitchProcessor {
    rx: Receiver<PitchData>
}

impl PitchProcessor {
    pub const PITCH_TOLERANCE: f32 = 0.8;

    pub fn new(pitch_tolerance: Option<f32>) -> Self {
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let stream = AudioStream::new();

            let mut pitch_detector = Pitch::new(PitchMode::Yin, CHUNK_SIZE, CHUNK_SIZE, SAMPLING_RATE).unwrap();
            pitch_detector.set_tolerance(pitch_tolerance.unwrap_or(PitchProcessor::PITCH_TOLERANCE));
            
            let mut last_data = PitchData::default();

            loop {
                let samples = stream.receiver.recv().unwrap();
                let hann_window = hann_window(&samples);
                let pitch = pitch_detector.do_result(&hann_window).unwrap();
                
                let pitch_data = PitchData {
                    pitch: pitch,
                    last_pitch: last_data.pitch
                };

                tx.send(pitch_data).unwrap();
                last_data = pitch_data;
            }
        });

        Self {
            rx
        }
    }

    pub fn drain(proc: &PitchProcessor) -> Option<PitchData> {
        let mut latest: Option<PitchData> = None;
        
        while let Ok(pitch_data) = proc.rx.try_recv() {
            latest = Some(pitch_data);
        }

        latest
    }
}