
from app.main import create_app
from app.routes.web import register_web_routes





if __name__ == '__main__':
    app = create_app()
    register_web_routes(app)
    app.run(debug=True, host='0.0.0.0', port=5000)