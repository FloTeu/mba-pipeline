from django.http import HttpResponse

def homepage(request):
    return HttpResponse("Homepage")

def about(request):
    return HttpResponse("about")


