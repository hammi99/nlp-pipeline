import pathlib
import streamlit as st
import pandas as pd


st.set_page_config(
	page_title= 'Sentiment Prediction Dashboard', 
	page_icon = '📈', 
	layout    = 'wide', 
	initial_sidebar_state= 'expanded', 
)


prediction1day = pd.read_csv('./data/1-day-sentiment-prediction.csv')
prediction3day = pd.read_csv('./data/3-day-sentiment-prediction.csv')
prediction7day = pd.read_csv('./data/7-day-sentiment-prediction.csv')



with st.sidebar:
    st.title('select Sentiment scores to display')
    features = {
        'compound': st.checkbox('compound', value= True),
        'negative': st.checkbox('negative', value= True),
        'neutral' : st.checkbox('neutral' , value= True),
        'positive': st.checkbox('positive', value= True),
	}
    features = [key   for key, value in features.items() if value]
    columns  = [colName 
		for colName in prediction1day.columns 
        if any(
            feature in colName
            for feature in features
		)
    ]

st.write('# Sentiment Prediction')
col = st.columns((2, 2), gap='medium')

with col[0]:
	st.write('### 1-day prediction')
	st.line_chart(
		data= prediction1day, 
		x     = 'createdAt', 
		y     = columns,
		height= 500, 
	)
with col[1]:
	st.write('### 3-day prediction')
	st.line_chart(
		data= prediction1day, 
		x     = 'createdAt', 
		y     = columns,
		height= 500, 
	)
# with col[1]:

st.write('### 7-day prediction')
st.line_chart(
	data= prediction1day, 
	x     = 'createdAt', 
	y     = columns,
	height= 500, 
)