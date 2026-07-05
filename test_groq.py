#!/usr/bin/env python3
"""
Simple test script to verify Groq API functionality
"""

import os
import requests
import json

def test_groq_api():
    """Test Groq API with a simple sentiment analysis"""
    
    # Get API token from environment
    api_token = os.getenv('GROQ_API_TOKEN', '')
    
    if not api_token:
        print("❌ No GROQ_API_TOKEN found in environment")
        return False
    
    print(f"✅ API Token found (length: {len(api_token)})")
    
    # Test API call
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": [
            {
                "role": "user",
                "content": "What is the sentiment of this text: 'I love badinka clothing, it's amazing quality!' Answer with one word: positive, negative, or neutral"
            }
        ],
        "max_tokens": 20,
        "temperature": 0.0
    }
    
    try:
        print("📡 Sending test request to Groq API...")
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        
        print(f"📊 Response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            print(f"✅ Groq API working! Response: '{content}'")
            return True
        else:
            print(f"❌ Groq API error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Groq API: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Groq API functionality...")
    success = test_groq_api()
    if success:
        print("✅ All tests passed!")
    else:
        print("❌ Tests failed!")