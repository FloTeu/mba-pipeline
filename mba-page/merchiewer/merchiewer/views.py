from django.http import HttpResponse

def homepage(request):
    return HttpResponse("Ich geh dir fremd :O")

def about(request):
    return HttpResponse("about")


