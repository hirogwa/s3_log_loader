import boto3
import click
import models
import settings

client = boto3.client(
    's3',
    region_name=settings.S3_REGION,
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
)


def replace_delimiter(s, delim='\n'):
    original_delim = ' '
    openers = ['[', '"']
    closers = [']', '"']
    inside_block = False
    target_indices = []
    for i, c in enumerate(s):
        if c in openers and s[i-1] == original_delim:
            inside_block = True
            continue
        if c in closers and s[i+1] == original_delim:
            inside_block = False
            continue
        if c == original_delim and not inside_block:
            target_indices.append(i)
    return ''.join(
        [delim if i in target_indices else x for i, x in enumerate(s)])


def load_log(bucket, key, dry_run):
    click.echo(key)
    session = models.Session()
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj.get('Body').read().decode('utf-8')
    for x in body.split('\n')[:-1]:
        delim = '\n'
        line = replace_delimiter(x, delim)
        entry = models.LogEntryRaw(*line.split(delim))
        if not dry_run:
            session.add(entry)

    if not dry_run:
        session.commit()
        client.delete_object(Bucket=bucket, Key=key)


def extract(bucket, max_keys=1000, prefix='', dry_run=False):
    response = client.list_objects_v2(
        Bucket=bucket,
        EncodingType='url',
        MaxKeys=max_keys,
        Prefix=prefix
    )

    keys = [x.get('Key') for x in response.get('Contents', [])]
    for key in keys:
        load_log(bucket, key, dry_run)
    click.echo('keys processed: {} {}'.format(
        len(keys), '(dry run)' if dry_run else ''))


@click.command()
@click.option('-d', '--dry-run/--no-dry-run', default=False)
@click.option('-p', '--prefix', default='logs/')
@click.option('-c', '--max-count', default=100)
def main(max_count, prefix, dry_run):
    extract(settings.LOG_BUCKET, max_count, prefix, dry_run)


if __name__ == '__main__':
    main()
