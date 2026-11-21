"""GitHub Actions scheduled collection. No AI key required for RSS collection."""
from news_core import collect,connect,validate

if __name__ == '__main__':
    book=connect()
    problems=validate(book)
    if problems:raise SystemExit('Invalid headers: '+ '; '.join(problems))
    result=collect(book,30)
    print('Collect summary:',result)
    if result['errors'] and result['errors']==result['sources']:
        raise SystemExit('All configured feeds failed')
