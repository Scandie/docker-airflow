import argparse

from sqlalchemy.exc import IntegrityError


def create_user(args):
    from airflow.contrib.auth.backends.password_auth import PasswordUser
    from airflow import models, settings

    u = PasswordUser(models.User())
    u.username = args.username
    u.email = args.email
    u.password = args.password

    s = settings.Session()
    s.add(u)
    try:
        s.commit()
    except IntegrityError:
        # User already exists
        pass
    finally:
        s.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', dest='username', nargs='?', help="Defaults to local part of email", default='username')
    parser.add_argument('-e', dest='email', default='username@mail.com')
    parser.add_argument('-p', dest='password', default='password')
    args = parser.parse_args()

    if not args.username:
        # Default username is the local part of the email address
        args.username = args.email[:args.email.index('@')]

    create_user(args)
