# scripts/rag/ui.py
import streamlit as st
from scripts.rag.agent import agent_executor

st.title("🏛️ Legislative AI Analyst")
st.caption("Hybrid RAG + SQL engine powered by DuckDB and LangChain")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input("Ask about bills, members, or legislation..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing database and vector embeddings..."):
            result = agent_executor.invoke({"messages": [("user", prompt)]})
            answer = result["messages"][-1].content
            st.write(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})