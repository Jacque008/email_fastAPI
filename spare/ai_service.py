"""
AI service - Handle all AI related calls
"""
import os
import re
from typing import List, Dict
from groq import Groq
import pandas as pd
from ..app.services.base_service import BaseService

class AIService(BaseService):
    """AI service"""
    
    def __init__(self):
        super().__init__()
        self.groq_client = self._initialize_groq_client()
        self.model = self._load_model()
        self.system_prompts = self._load_system_prompts()
    
    def _initialize_groq_client(self) -> Groq:
        """Initialize Groq client"""
        api_key = os.getenv('GROQ_API_KEY')
        if not api_key:
            raise ValueError("GROQ_API_KEY environment variable not set")
        return Groq(api_key=api_key)
    
    def _load_model(self) -> str:
        """Load AI model configuration"""
        try:
            model_df = pd.read_csv(f"{self.folder}/model.csv")
            return model_df['model'].iloc[0]
        except:
            return "llama-3.3-70b-versatile"
    
    def _load_system_prompts(self) -> Dict[str, str]:
        """Load system prompts"""
        return {
            "summary": (
                "You are a professional and efficient assistant from Sweden, "
                "specializing in accurately summarizing conversations in Swedish. "
                "Your summaries must be clear, concise, accurate, and well-structured. "
                "Format the summary as bullet points, starting each point with '*   '. "
                "Ensure precision and avoid unnecessary information. "
                "Do not translate the text.\n\n"
                "Background information: "
                "- 'Clinic' refers to veterinary clinics that provide medical care for pets. "
                "- 'Insurance company' refers to the entity responsible for processing claims. "
                "- 'DRP' acts as an intermediary platform connecting clinics and insurance companies."
            ),
            "risk_assessment": (
                "You are an expert in log analysis and risk assessment. "
                "Your task is to analyze errand logs in Swedish and provide risk assessment. "
                "Focus on identifying missing steps, time issues, payment discrepancies, "
                "exceptions, and special risk warnings."
            )
        }
    
    def generate_conversation_summary(self, formatted_text: str) -> str:
        """Generate conversation summary"""
        user_prompt = (
            "Summarize the following conversation between the clinic, insurance company, "
            "and DRP concisely and accurately in Swedish. "
            "Capture only the key points from the conversations. "
            "Replace 'insurance company' with 'FB' in all instances. "
            "If DRP only forwarded an email, do not mention it in the summary. "
            "Ensure the summary is concise, well-structured, and focuses on key points.\n\n"
            f"{formatted_text}"
        )
        
        return self._get_ai_response([
            {"role": "system", "content": self.system_prompts["summary"]},
            {"role": "user", "content": user_prompt}
        ])
    
    def perform_risk_assessment(self, log_content: str) -> str:
        """Perform risk assessment"""
        user_prompt = f"Analyze the following errand log and provide risk assessment in Swedish: {log_content}"
        
        return self._get_ai_response([
            {"role": "system", "content": self.system_prompts["risk_assessment"]},
            {"role": "user", "content": user_prompt}
        ])
    
    def _get_ai_response(self, messages: List[Dict]) -> str:
        """Get AI response"""
        try:
            chat_completion = self.groq_client.chat.completions.create(
                messages=messages,
                model=self.model
            )
            response = chat_completion.choices[0].message.content
            
            # Clean response format
            parts = re.split(r':\s*', response)
            if len(parts) > 1:
                response = ":".join(parts[1:])
            
            return response
            
        except Exception as e:
            return f"AI analysis error: {str(e)}"
