import requests
import json
from dotenv import load_dotenv
import os
from datetime import date

# Load API key from .env file
load_dotenv(dotenv_path=".env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
MAX_RESULTS = 50

# -----------------------------
# Function to get the uploads playlist ID of a YouTube channel
# -----------------------------
def get_playlist_id(channel_handle):
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Extract uploads playlist ID
        playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return playlist_id

    except requests.exceptions.RequestException as e:
        raise e

# -----------------------------
# Function to get all video IDs from a playlist
# -----------------------------
def get_video_ids(playlist_id, max_results=MAX_RESULTS):
    video_ids = []
    page_token = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults={max_results}&key={API_KEY}"

    try:
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Collect video IDs
            for item in data.get("items", []):
                vid_id = item.get("contentDetails", {}).get("videoId")
                if vid_id:
                    video_ids.append(vid_id)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

    except requests.exceptions.RequestException as e:
        raise e

    return video_ids

# -----------------------------
# Helper function to split a list into batches
# -----------------------------
def batch_list(lst, batch_size=MAX_RESULTS):
    """Yield successive batches from a list"""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

# -----------------------------
# Function to extract key video details for each video ID
# -----------------------------
def extract_video_data(video_ids):
    extracted_data = []

    try:
        # Loop through batches of video IDs
        for batch in batch_list(video_ids, batch_size=MAX_RESULTS):
            # Create comma-separated string of video IDs
            video_id_str = ",".join(batch)

            # Construct URL for YouTube videos API
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={video_id_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Loop through each video in response
            for item in data.get("items", []):
                video_data = {
                    "video_id": item.get("id"),
                    "title": item.get("snippet", {}).get("title"),
                    "published_at": item.get("snippet", {}).get("publishedAt"),
                    "duration": item.get("contentDetails", {}).get("duration"),
                    "view_count": item.get("statistics", {}).get("viewCount"),
                    "like_count": item.get("statistics", {}).get("likeCount"),
                    "comment_count": item.get("statistics", {}).get("commentCount")
                }

                # Replace missing statistics with None
                for key in ["view_count", "like_count", "comment_count"]:
                    if video_data[key] is None:
                        video_data[key] = None

                extracted_data.append(video_data)

    except requests.exceptions.RequestException as e:
        raise e

    return extracted_data

# -----------------------------
# Function to save extracted data to JSON
# -----------------------------
def save_to_json(extracted_data):
    file_path = f"./data/youtube_data_{date.today()}.json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure folder exists

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)

# -----------------------------
# Main execution
# -----------------------------
if __name__ == "__main__":
    # Step 1: Get uploads playlist ID
    playlist_id = get_playlist_id(CHANNEL_HANDLE)
    # Step 2: Get all video IDs from the playlist
    video_ids = get_video_ids(playlist_id)
    # Step 3: Extract all video details
    videos_info = extract_video_data(video_ids)
    # Step 4: Save extracted data to JSON
    save_to_json(videos_info)
    # Optional: print first 5 video entries
    for v in videos_info[:5]:
        print(json.dumps(v, indent=4, ensure_ascii=False))
