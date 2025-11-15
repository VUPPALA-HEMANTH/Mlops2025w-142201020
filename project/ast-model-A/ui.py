# import gradio as gr
# import torch
# import numpy as np
# import librosa
# from transformers import AutoModelForAudioClassification, AutoProcessor

# MODEL_PATH = "/home/hemanth/Desktop/HEMANTH/SEM-7/MLOPS/Project/kaggle/ast-model-A"

# model = AutoModelForAudioClassification.from_pretrained(MODEL_PATH)
# processor = AutoProcessor.from_pretrained(MODEL_PATH)

# def predict_emotion(audio):
#     if audio is None:
#         return "No audio detected."

#     # Your Gradio version returns (sample_rate, waveform)
#     sr, audio_array = audio

#     # Convert int16 → float32
#     audio_array = audio_array.astype(np.float32)

#     # Stereo → mono
#     if len(audio_array.shape) > 1:
#         audio_array = np.mean(audio_array, axis=1)

#     # Resample
#     if sr != 16000:
#         audio_array = librosa.resample(audio_array, orig_sr=sr, target_sr=16000)
#         sr = 16000

#     # Process audio
#     inp = processor(audio_array, sampling_rate=sr, return_tensors="pt")

#     # Predict
#     with torch.no_grad():
#         logits = model(**inp).logits
#         pred_id = int(torch.argmax(logits, dim=-1))

#     # CONFIG USES INTEGER KEYS
#     return model.config.id2label[pred_id]

# ui = gr.Interface(
#     fn=predict_emotion,
#     inputs=gr.Audio(sources=["microphone"], type="numpy"),
#     outputs="text",
#     title="🎤 Live Emotion Detection",
#     description="Record audio to detect emotion.",
# )

# ui.launch()


import gradio as gr
import torch
import numpy as np
import librosa
from transformers import AutoModelForAudioClassification, AutoProcessor

MODEL_PATH = "/home/hemanth/Desktop/HEMANTH/SEM-7/MLOPS/Project/kaggle/ast-model-A"

model = AutoModelForAudioClassification.from_pretrained(MODEL_PATH)
processor = AutoProcessor.from_pretrained(MODEL_PATH)

def predict_emotion(audio):
    if audio is None:
        return "No audio detected."

    # Your Gradio format: (sample_rate, waveform)
    sr, audio_array = audio

    # Convert int16 → float32
    audio_array = audio_array.astype(np.float32)

    # Stereo → mono
    if len(audio_array.shape) > 1:
        audio_array = np.mean(audio_array, axis=1)

    # Resample
    if sr != 16000:
        audio_array = librosa.resample(audio_array, orig_sr=sr, target_sr=16000)
        sr = 16000

    # Preprocess
    inputs = processor(audio_array, sampling_rate=sr, return_tensors="pt")

    with torch.no_grad():
        logits = model(**inputs).logits
        probs = torch.softmax(logits, dim=-1)[0].cpu().numpy()

    # RETURN FLOATS (no % symbols)
    labels = model.config.id2label
    result = {labels[i]: float(probs[i]) for i in range(len(probs))}

    return result

ui = gr.Interface(
    fn=predict_emotion,
    inputs=gr.Audio(sources=["microphone"], type="numpy"),
    outputs=gr.Label(num_top_classes=5),   # Shows bars + top classes
    title="🎤 Live Emotion Detection",
    description="Record audio and see the emotion probabilities.",
)

ui.launch(share=True)
