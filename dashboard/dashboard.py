import pathlib
import streamlit as st
import pandas as pd

import copyData	# copies data from mongodb to local csv files



df = pd.concat([
    pd.read_csv(file, parse_dates= [0], infer_datetime_format= True)
    
	for file in pathlib.Path('./Sentiment').iterdir()
    if file.suffix == '.csv'
])

df.reset_index(drop= True, inplace= True)
# df.set_index('createdAt', inplace= True)
# df.drop(columns= ['_id', 'userName'], inplace= True)
df.drop(columns= 'createdAt', inplace= True)

print(df.info())


df = df.rolling(window= 10).mean()




with st.sidebar:
    st.title('select Sentiment scores to display')
    columns = {
        'compound': st.checkbox('compound', value= True),
        'negative': st.checkbox('negative', value= True),
        'neutral' : st.checkbox('neutral' , value= True),
        'positive': st.checkbox('positive', value= True),
	}
    colors = {
        'compound': '#0E1FF0',
		'negative': '#F00E0E',
		'neutral' : '#7D7D7D',
		'positive': '#5DF00E',
	}
    
    columns = [key   for key, value in columns.items() if value]
    colors  = [value for key, value in colors.items()  if key in columns]
	# st.write(columns)
    

st.line_chart(
    data= df, 
    # x     = 'createdAt', 
    y     = columns,
    color = colors,
    height= 500, 
)