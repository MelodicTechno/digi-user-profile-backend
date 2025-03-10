from django.http import JsonResponse
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.views.decorators.http import require_http_methods

# 登录
@require_http_methods(["POST"])
def user_login(request):
    try:
        user_id = request.POST.get('user_id')
        user = User.objects.get(id=user_id)
        login(request, user)
        return JsonResponse({
            'status': 'success',
            'message': '登录成功',
            'user_id': user.id,
            'username': user.username,
        })
    except User.DoesNotExist:
        return JsonResponse({
            'status': 'error',
            'message': '无效的用户 ID',
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e),
        }, status=500)

# 注册
@require_http_methods(["POST"])
def user_register(request):
    try:
        id = request.POST.get('id')

        if not id:
            return JsonResponse({
                'status': 'error',
                'message': 'ID是必填项',
            }, status=400)

        if User.objects.filter(id=id).exists():
            return JsonResponse({
                'status': 'error',
                'message': '用户已存在',
            }, status=400)

        user = User.objects.create(
            id=id
        )
        return JsonResponse({
            'status': 'success',
            'message': '注册成功',
            'user_id': user.id,
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e),
        }, status=500)