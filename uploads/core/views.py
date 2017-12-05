from django.shortcuts import render, redirect
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.http import JsonResponse, HttpResponse

from uploads.core.models import Document
from uploads.core.forms import DocumentForm
from uploads.core.dedupetrainer import Dedupetrainer
import pandas as pd
import json 

def home(request):
    documents = Document.objects.all()
    return render(request, 'core/home.html', { 'documents': documents })


def simple_upload(request):
    if request.method == 'POST' and request.FILES['myfile']:
        myfile = request.FILES['myfile']
        fs = FileSystemStorage()
        filename = fs.save(myfile.name, myfile)
        uploaded_file_url = fs.url(filename)
        return render(request, 'core/simple_upload.html', {
            'uploaded_file_url': uploaded_file_url
        })
    return render(request, 'core/simple_upload.html')

def headers_view(request):
    if request.method == 'GET':
        a = pd.read_csv("./media/SampleData.csv")
        headers = a.columns
        return render(request, 'core/headers.html', {
            'headers': headers
        })
    if request.method == 'POST': 
        response = {}
        request_params = json.loads(request.body.decode('utf-8'))
        print('request body', request_params)
        filename = "./media/"+ request_params["input_file"]
        # a = pd.read_csv(filename)        
        # headers = a.columns
        # response ["columns"] = headers.tolist()
        response["value"] = Dedupetrainer.train(filename,request_params["sample_size"],request_params["fields"])
        return JsonResponse(response)
    return HttpResponse("error in request",status=500)

def model_form_upload(request):
    if request.method == 'POST':
        form = DocumentForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('home')
    else:
        form = DocumentForm()
    return render(request, 'core/model_form_upload.html', {
        'form': form
    })
