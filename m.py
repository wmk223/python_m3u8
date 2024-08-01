import time
import random
import uuid
import os
import ffmpeg
import threading

from PIL import Image
from moviepy.editor import *
from flask import Flask, jsonify, request
from flask_executor import Executor
from flask_sqlalchemy import SQLAlchemy


BASE_DIR = os.path.join(os.path.dirname(__file__))
app = Flask(__name__, static_folder=os.path.join(BASE_DIR, 'upload'))

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:root@localhost:3306/mianshao'
app.config['SQLALCHEMY_POOL_SIZE'] = 10
app.config['SQLALCHEMY_POOL_TIMEOUT'] = 30
app.config['SQLALCHEMY_POOL_RECYCLE'] = 240
db = SQLAlchemy(app)


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def random_str(num=6):
    uln = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    rs = random.sample(uln, num)  # 生成一个 指定位数的随机字符串
    a = uuid.uuid1()  # 根据 时间戳生成 uuid , 保证全球唯一
    b = ''.join(rs + str(a).split("-"))  # 生成将随机字符串 与 uuid拼接
    return b  # 返回随机字符串

@app.route('/', methods=['GET'])
def index():
    return '''
        <!doctype html>
        <title>上传文件</title>
        <h1>上传文件</h1>
        <form action="/upload" method=post enctype=multipart/form-data>
          <input type=file name=file>
          <input type=submit value=上传>
        </form>
        '''

@app.route('/upload', methods=['POST'])
def upload():  # 接收前端上传

    # 测试
    # threading.Thread(target=long_running_task(new_file)).start()
    # executor.submit(do_task, 'hello')

    file = request.files['file']
    file_name = os.path.join(BASE_DIR, 'upload/') + file.filename

    # 写入文件
    file.save(file_name)

    new_file = {
        'url': request.host_url + 'upload/' + file.filename,
        'name': file.filename,
    }
    content_type = file.content_type.rsplit("/", 1)[0].lower()
    file.close()

    if content_type == 'video':
        file_path = 'upload/' + time.strftime("%Y%m%d%H%M%S", time.localtime()) + '/'
        m3u8_name = random_str(1) + ".m3u8"
        obj = {
            'file_name': file_name,
            'm3u8_name': m3u8_name,
            'file_path': file_path,
            'pic_path': random_str(1) + ".jpg"
        }
        new_file = {
            'url': request.host_url + file_path + m3u8_name,
            'name': m3u8_name,
            'pic_path':request.host_url + file_path + obj['pic_path']
        }
        #同步执行
        #threading.Thread(target=threading_task(obj)).start()
        #异步执行
        executor.submit(executor_task, obj)

    return jsonify({'code': 200, 'data': new_file})

def executor_task(obj):
    # 模拟耗时的操作
    file_path = os.path.join(BASE_DIR, obj['file_path'])
    mkdir(file_path)
    generate_video_cover(obj)
    ts = os.path.join(file_path, '%03d.ts')
    segment_list = os.path.join(file_path, obj['m3u8_name'])
    # 长时间任务的代码，例如处理大量数据
    stream = ffmpeg.input(obj['file_name'])
    stream = ffmpeg.output(stream, ts, vcodec="copy", acodec="copy", map=0,
                           f="segment", s="1280x720",
                           segment_list=segment_list, segment_time=5)
    orr, err = stream.overwrite_output().run()
    if err:
        os.remove(file_path)
    else:
        os.remove(obj['file_name'])
    return 1

def threading_task(obj):
    file_path = os.path.join(BASE_DIR, obj['file_path'])
    mkdir(file_path)
    generate_video_cover(obj)
    ts = os.path.join(file_path, '%03d.ts')
    segment_list = os.path.join(file_path, obj['m3u8_name'])
    # 长时间任务的代码，例如处理大量数据
    stream = ffmpeg.input(obj['file_name'])
    stream = ffmpeg.output(stream, ts, vcodec="copy", acodec="copy", map=0,
                           f="segment", s="1280x720",
                           segment_list=segment_list, segment_time=5)
    orr, err = stream.overwrite_output().run()
    if err:
        os.remove(file_path)
    else:
        os.remove(obj['file_name'])
    pass

def generate_video_cover(obj):
    file_path = os.path.join(BASE_DIR, obj['file_path'])
    video = VideoFileClip(obj['file_name'])
    frame = video.get_frame(1)
    image = Image.fromarray(frame)
    image.save(os.path.join(file_path, obj['pic_path']))
    image.close()
    video.close()

if __name__ == '__main__':
    executor = Executor(app)
    app.run(debug=False, threaded=True)



# input_video = "https://liverecorddg.kpinfo.cn/broadcast/activity/1720599482522105/record.m3u8"
# output_video = os.path.join(os.path.dirname(__file__),"222.mp4")
#
# # r = requests.get(input_video)
# # print(r.status_code)
#
# # playlist = m3u8.load(input_video)
# # video_urls = [segment.uri for segment in playlist.segments]
#
# stream = ffmpeg.input(input_video)
# stream = ffmpeg.output(stream, output_video)
# ffmpeg.run(stream)




# os.makedirs('./333')

# stream = ffmpeg.input('https://filekp.ccwb.cn/api/file/20240725151715D1E8FU.mp4')
# stream = ffmpeg.output(stream, './333/333_%03d.ts', vcodec="copy", acodec="copy", map=0,
#                        f="segment", s="1280x720",
#                        segment_list="./333/11.m3u8", segment_time=5)
# out, err = stream.overwrite_output().run()
# print(out)


