import gradio as gr

def debug(audio):
    print("\n\n=== RAW AUDIO INPUT RECEIVED ===")
    print(audio)
    print("TYPE:", type(audio))
    return "Check your terminal output."

ui = gr.Interface(
    fn=debug,
    inputs=gr.Audio(sources=["microphone"], type="numpy"),
    outputs="text",
)

ui.launch()
