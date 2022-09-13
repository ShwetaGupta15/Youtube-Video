import numpy as np
import pandas as pd
import sys
import os
import json
import csv
import regex as re
import kaggle
import argparse


def getPathLists(args):
	global videos_path_list
	videos_path_list = []
	global categories_path_list
	categories_path_list = []
	for root,dirs,file_list in os.walk(args.archivePath):
		for file in file_list:
			if ('category' in file):
				categories_path_list.append(os.path.join(root,file))
			elif ('video' in file):
				videos_path_list.append(os.path.join(root,file))
	return videos_path_list,categories_path_list

def getCategoriesDF():
	category_list = []
	#print(categories_path_list)
	for category_file in categories_path_list:
		with open(category_file) as json_file:
			category = json.load(json_file)
		for item_c in category['items']:
			temp_dict = dict()
			temp_dict['category_id'] = item_c['id']
			temp_dict['Category_Title'] = item_c['snippet']['title']
			temp_dict['Assignable'] = item_c['snippet']['assignable']
			temp_dict['Region'] = category_file.split('\\')[-1].split('_')[0]
			category_list.append(temp_dict)
			del temp_dict
	category_df = pd.DataFrame(category_list)
	category_df['category_id'] = category_df.category_id.astype(int)
	category_df['Region'] = category_df.Region.astype(str)
	return category_df

def getVideosDF():
	videos_list = []
	#print(videos_path_list)
	for video_file in videos_path_list:
		region = re.sub('[^A-Z]', '', video_file.split('\\')[-1])
		temp_df = pd.read_csv(video_file,encoding='latin-1')
		temp_df['Region'] = region
		temp_dict = temp_df.to_dict('records')
		videos_list.extend(temp_dict)
		del temp_df,temp_dict
	videos_df = pd.DataFrame(videos_list)
	videos_df['Region'] = videos_df.Region.astype(str)
	return videos_df

def finalData(videos_df,category_df,args):
	final_df = category_df.merge(videos_df, how = 'inner',left_on = ['category_id','Region'],right_on = ['category_id','Region'])
	final_df = final_df.drop(columns=['tags'])
	final_df = final_df.rename(columns={'video_id':'id','title':'video_title'})
	final_df['id'] = final_df['id'].astype(str)
	#final_df.desc(10)
	#final_df.to_csv(args.outPath,index=False,header=True)
	
	
	file_path = args.outPath + 'finalDataset.json'
	if os.path.exists(file_path):
		print("deleting old json")
		os.remove(file_path)
	print("Unloading data")
	final_df.to_json(file_path,orient='records')


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Process the YouTube data')
	parser.add_argument('--archivePath',type=str,required=True)
	parser.add_argument('--outPath',type=str,required=True)
	args = parser.parse_args()
	os.system('kaggle datasets download -d datasnaek/youtube-new -p ' + args.archivePath + ' --unzip -o -q')
	getPathLists(args)
	category_df = getCategoriesDF()
	videos_df = getVideosDF()
	finalData(videos_df,category_df,args)
	print("data unloaded")
