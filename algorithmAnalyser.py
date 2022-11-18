import pandas as pd
from io import BytesIO
import base64
from PIL import Image
import matplotlib.pyplot as plt
plt.switch_backend('agg')
from pycaret.regression import *
# from pycaret.classification import *
# from pycaret.clustering import *
# from pycaret.anomaly import *

def regression(file,target):
    resp = {}
    try:
        print('Analysis starting...')
        if file.content_type == 'text/csv':
            file.save('temp.csv')
            df = pd.read_csv('temp.csv')
            # df = spark.read.csv('temp.csv', header=True, inferSchema=True)
        else:
            file.save('temp.xlsx')
            df = pd.read_excel('temp.xlsx')
            # df = data.to_spark()
        print('File read')
        setup1 = setup( data = df , target = target, silent=True)
        print('Setup done...')
        compare = compare_models()
        create = create_model(compare)
        print('Compare done...')
        plot = plot_model(create, save=True)
        plot_bytes_image = BytesIO(b'')
        plot_fig = Image.open(plot)
        plot_fig.save(plot_bytes_image, format='png')
        plot_bytes_image.seek(0)
        resp['Residual'] = base64.b64encode(plot_bytes_image.getvalue()).decode()
        plt.clf()
        plot = plot_model(create, plot='error', save=True)
        plot_bytes_image = BytesIO(b'')
        plot_fig = Image.open(plot)
        plot_fig.save(plot_bytes_image, format='png')
        plot_bytes_image.seek(0)
        resp['Error'] = base64.b64encode(plot_bytes_image.getvalue()).decode()
        plt.clf()
        plot = plot_model(create, plot='feature', save=True)
        plot_bytes_image = BytesIO(b'')
        plot_fig = Image.open(plot)
        plot_fig.save(plot_bytes_image, format='png')
        plot_bytes_image.seek(0)
        resp['Feature'] = base64.b64encode(plot_bytes_image.getvalue()).decode()
        plt.clf()
        print('Plot done...')
        interpret = interpret_model(compare, save=True)
        interpret_bytes_image = BytesIO(b'')
        interpret_fig = Image.open('SHAP summary.png')
        interpret_fig.save(interpret_bytes_image, format='png')
        interpret_bytes_image.seek(0)
        resp['Interpret'] = base64.b64encode(interpret_bytes_image.getvalue()).decode()
        plt.clf()
        print('Interpret done...')
        compare = pull()
        resp['CompareHeader'] = list(compare)
        resp['CompareValue'] = compare.values.tolist()
    except Exception as e:
        print(e)
        resp['error'] = str(e)
    print('Analysis done!')
    return resp