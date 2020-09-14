
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/5/18 10:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : Messager.py
"""
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()

if config.mattermost_open:
    import lpmln.message.MatterMost as mm

import logging


def send_message(msg, attached_files=list()):
    if config.mattermost_open:
        mm_server = config.mattermost_server
        mm_port = config.mattermost_port
        user = config.mattermost_username
        pwd = config.mattermost_password

        api_token = config.mattermost_api_token
        channel_id = config.mattermost_channel_id

        try:
            driver = mm.login(mm_server, mm_port, user, pwd, api_token)
            mm.post_message(driver, msg=msg, channel_id=channel_id, files=attached_files)
            driver.logout()
        except Exception as e:
            logging.error(e)
            logging.error("cannot send messages to mattermost sever %s" % mm_server)


if __name__ == '__main__':
    print(mm)
    send_message("hh")
    pass
    