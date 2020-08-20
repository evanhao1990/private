from flask import Flask, render_template, url_for
app = Flask(__name__)

posts = [
    {
        'author': 'Hao',
        'title': 'Blog post 1',
        'content':'First post content',
        'date_posted': ' July 31, 2020'
    },
    {
        'author': 'Jack',
        'title': 'Blog post 2',
        'content': 'First post content',
        'date_posted': ' May 31, 2020'
    },
    {
        'author': 'Rose',
        'title': 'Blog post 3',
        'content': 'First post content',
        'date_posted': ' Feb 31, 2020'
    }
]

title = 'About'


@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html', posts=posts)


@app.route('/about')
def about():
    return render_template('about.html', title=title)


if __name__ == '__main__':
    app.run(debug=True)