import streamlit as st
from streamlit_mic_recorder import mic_recorder
from openai import OpenAI
import os
import io

def handle_voice_input():
    """
    Renders the mic button and returns transcribed text with console logging.
    """
    client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    # 1. Render the Mic Button
    audio = mic_recorder(
        start_prompt="🎤 Click to Speak",
        stop_prompt="🛑 Stop",
        just_once=True,
        use_container_width=False,
        key='voice_input'
    )

    if audio and 'bytes' in audio:
        print(f"LOG: Audio data received ({len(audio['bytes'])} bytes).")
        try:
            # 2. Prepare audio for OpenAI
            audio_bio = io.BytesIO(audio['bytes'])
            audio_bio.name = "audio.wav"
            
            # 3. Transcribe using Whisper
            with st.spinner("Translating voice to text..."):
                print("LOG: Sending audio to OpenAI Whisper...")
                transcript = client_ai.audio.transcriptions.create(
                    model="whisper-1", 
                    file=audio_bio
                )
                
                # THIS IS THE KEY LOG:
                print(f"LOG: Whisper Transcript: '{transcript.text}'")
                
                return transcript.text
        except Exception as e:
            print(f"LOG ERROR: Whisper transcription failed: {e}")
            st.error(f"Voice Error: {e}")
            return None
    
    return None