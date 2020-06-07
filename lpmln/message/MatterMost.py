#script (python)
# encoding:utf-8
"""
@author: 王彬
@date:
@describe:
"""

from mattermostdriver import Driver
import lpmln.config.GlobalConfig as cfg

config = cfg.load_configuration()


def login(url, port, username, password, token):
    driver = Driver({
        'url': url,
        'port': int(port),
        'login_id': username,
        'password': password,
        'basepath': '/api/v4',
        'scheme': 'http',
        'token': token
    })

    driver.login()
    return driver


def logout(connector = Driver()):
    connector.logout()


def get_user_info_by_username(connector=Driver(), user_name=config.mattermost_username):
    user = connector.users.get_user_by_username(username=user_name)
    print("user name", user["username"], "user id", user["id"])
    return user


def get_team_info_by_team_id(connector=Driver(), team_id=""):
    team = connector.teams.get_team(team_id)
    print("team name", team["display_name"])
    return team


def post_message(connector=Driver(), msg="", channel_id="", mention="@channel", files=list()):
    file_ids = list()

    for f in files:
        file_ids.append(upload_file(connector, channel_id, f))

    connector.posts.create_post({
        "message": mention+" "+msg,
        "channel_id": channel_id,
        "file_ids": file_ids
    })


def upload_file(connector=Driver(), channel_id="", path=None):
    if path is not None:
        f = connector.files.upload_file(channel_id, {"files":open(path, "rb"), "filename":path})
        return f["file_infos"][0]["id"]
    else:
        return None


if __name__ == '__main__':
    mm_server = config.mattermost_server
    mm_port = config.mattermost_port
    user = config.mattermost_username
    pwd = config.mattermost_password
    api_token = config.mattermost_api_token
    team_id = config.mattermost_team_id
    channel_id = config.mattermost_channel_id


    driver = login(mm_server, mm_port, user, pwd, api_token)
    me = get_user_info_by_username(driver, config.mattermost_username)
    print(me)
    team_users = driver.teams.get_team_members_for_user(me["id"])
    print(team_users)
    channels = driver.teams.get_public_channels(config.mattermost_team_id)
    print(channels)
    file = "W:/my_projects/qsr/qsr_output/0-1-0-isp-emp.txt"
    post_message(driver, msg="测试消息, test 123 1.1.1.1 !!!! \r\n test", channel_id=config.mattermost_channel_id)

    post_message(driver, msg="hello files ...", channel_id=config.mattermost_channel_id)

    post_message(driver, msg="hello files ...", channel_id=config.mattermost_channel_id, files=[file])
    