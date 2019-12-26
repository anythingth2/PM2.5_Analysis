# %%
from flask import Flask, request
# %%
app = Flask(__name__)

@app.route('/api/sensor/tgr', methods=['GET'])
def get_tgr_sensor():
    period = request.args.get('period')

    return {
        'history': f'i m {period}'
    }

# %%


if __name__ == '__main__':
    
    app.run(debug=True)
