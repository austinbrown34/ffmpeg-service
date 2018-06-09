from flask import Flask, jsonify, make_response, request, abort, Response
import subprocess
import uuid
import os
import json
import boto3
import botocore
import shutil
from zappa.async import task


app = Flask(__name__)
s3 = boto3.resource('s3')
client = boto3.client('s3')


def build_response(resp_dict, status_code):
    response = Response(json.dumps(resp_dict), status_code)
    return response


def get_images(images_url):
    # return list of image file names
    bucket = images_url.split('.com/')[1].split('/')[0]
    dist = images_url.split('.com/{}/'.format(bucket))[1]
    images = []
    my_bucket = s3.Bucket(bucket)
    print(bucket)
    print(dist)
    for object_summary in my_bucket.objects.filter(Prefix="{}".format(dist)):
        print(object_summary)
        images.append(object_summary.key)
    return images


def download_images(images_s3_url):
    # download images
    # https://s3.amazonaws.com/hiphy/images/01.jpg
    bucket = images_s3_url.split('.com/')[1].split('/')[0]
    dist = images_s3_url.split('.com/{}/'.format(bucket))[1]
    local = '/tmp/{}'.format(dist)
    try:
        shutil.rmtree(local)
    except OSError:
        pass
    os.makedirs(local)
    images = get_images(images_s3_url)
    images = [
        i for i in images if i.endswith('.jpg') or i.endswith('.png')
    ]
    for image in images:
        print (image)
        image_dest = os.path.join('/tmp', image)
        print (image_dest)
        s3.Bucket(bucket).download_file(image, image_dest)


def download_instructions(
        images_s3_url, instructions, dist, local, instructions_path):
    # download instructions
    # bucket = images_s3_url.split('.com/')[1].split('/')[0]

    try:
        os.remove(instructions_path)
    except OSError:
        pass
    with open(instructions_path, 'w') as f:
        for rule in instructions:
            f.write('{}\n'.format(rule))


def download_song(song, song_path, bucket):
    # download song
    print (song_path)
    try:
        try:
            os.remove(song_path)
        except OSError:
            pass
        s3.Bucket(bucket).download_file(song, song_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def create_video(instructions_path, song_path, video_path):
    # create video
    try:
        os.remove('/tmp/{}'.format('video.mp4'))
    except OSError:
        pass
    print(instructions_path)
    print(song_path)
    print(video_path)
    args = [
        'ffmpeg',
        '-f',
        'concat',
        '-safe',
        '0',
        '-i',
        instructions_path,
        '-i',
        song_path,
        video_path
    ]

    subprocess.call(args)


def upload_video(video_path, bucket):
    # upload video to s3
    session = boto3.Session()
    s3 = session.resource('s3')
    filename = 'video/{}.mp4'.format(uuid.uuid4().hex)
    s3.meta.client.upload_file(
        video_path,
        bucket,
        filename,
        {'ACL': 'public-read', 'ContentType': 'video/mp4'}
    )


@task
def transcode(data):
    BINARIES_FOLDER = '/bin/ffmpeg'
    LAMBDA_PATH = '{}:{}{}'.format(
        os.environ.get('PATH', ''),
        os.environ.get('LAMBDA_TASK_ROOT', ''),
        BINARIES_FOLDER
    )
    LAMBDA_LD_LIBRARY_PATH = '{}{}'.format(
        os.environ.get('LAMBDA_TASK_ROOT', ''),
        BINARIES_FOLDER
    )
    os.environ['PATH'] = LAMBDA_PATH
    os.environ['LD_LIBRARY_PATH'] = LAMBDA_LD_LIBRARY_PATH
    images_s3_url = data['images_s3_url']
    instructions = data['instructions']
    song_s3_url = data['song_s3_url']
    bucket = song_s3_url.split('.com/')[1].split('/')[0]
    song_path = '/tmp/{}'.format(song_s3_url.split('/')[-1])
    song = song_s3_url.split('.com/{}/'.format(bucket))[1]
    dist = images_s3_url.split('.com/{}/'.format(bucket))[1]
    local = '/tmp/{}'.format(dist)
    instructions_path = os.path.join(local, 'images.txt')
    video_path = '/tmp/video.mp4'
    download_images(images_s3_url)
    download_instructions(
        images_s3_url, instructions, dist, local, instructions_path)
    download_song(song, song_path, bucket)
    create_video(instructions_path, song_path, video_path)
    upload_video(video_path, bucket)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/')
def transcoder():
    return "Transcoder"


@app.route('/v1/convert', methods=['POST'])
def convert():
    print(request)
    if not (request.json):
        abort(400)
    print(request.json)
    data = request.json
    # print(request.data)
    # data = request.files['json']
    # file = request.files['file']
    # print(data)
    # print(file)
    transcode(data)

    return build_response({"status": "success"}, 200)


if __name__ == '__main__':
    app.run()
