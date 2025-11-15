from transformers import AutoModelForAudioClassification

MODEL_PATH = "/home/hemanth/Desktop/HEMANTH/SEM-7/MLOPS/Project/kaggle/ast-model-A"

model = AutoModelForAudioClassification.from_pretrained(MODEL_PATH)

print("=== id2label from model.config ===")
print(model.config.id2label)

print("=== label2id from model.config ===")
print(model.config.label2id)

print("num_labels =", model.config.num_labels)
