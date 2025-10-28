from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from typing import Optional, List
import json, os, threading
from fastapi import FastAPI
from admin_htpasswd import router as users_router

app = FastAPI(title="Users API")
app.include_router(users_router, prefix="/users-api")

DATA_PATH = "/var/lib/users_api/users.json"
_lock = threading.Lock()

def _load():
    if not os.path.exists(DATA_PATH): return {"seq": 1, "users": []}
    with open(DATA_PATH, "r", encoding="utf-8") as f: return json.load(f)

def _save(data):
    tmp = DATA_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, DATA_PATH)

app = FastAPI(title="Users Admin API")

class User(BaseModel):
    id: int
    username: str
    full_name: Optional[str] = None
    email: Optional[EmailStr] = None
    disabled: bool = False
    is_admin: bool = False

class NewUser(BaseModel):
    username: str
    full_name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: str
    is_admin: bool = False

@app.get("/admin/users", response_model=List[User])
def list_users():
    with _lock:
        return _load()["users"]

@app.post("/admin/users")
def create_user(u: NewUser):
    with _lock:
        data = _load()
        if any(x["username"] == u.username for x in data["users"]):
            raise HTTPException(409, "username exists")
        user = User(id=data["seq"], username=u.username, full_name=u.full_name,
                    email=u.email, is_admin=u.is_admin)
        data["users"].append(user.dict())
        data["seq"] += 1
        _save(data)
        return {"ok": True, "id": user.id}

@app.post("/admin/users/{id}/disable")
def disable_user(id: int, payload: dict):
    with _lock:
        data = _load()
        for x in data["users"]:
            if x["id"] == id:
                x["disabled"] = bool(payload.get("disabled", True))
                _save(data)
                return {"ok": True}
        raise HTTPException(404, "not found")

@app.post("/admin/users/{id}/reset_password")
def reset_pw(id: int, payload: dict):
    # senha não é persistida aqui; endpoint para acoplar no seu auth real
    if not payload.get("password"):
        raise HTTPException(400, "password required")
    with _lock:
        data = _load()
        if any(x["id"] == id for x in data["users"]):
            return {"ok": True}
    raise HTTPException(404, "not found")

@app.delete("/admin/users/{id}")
def delete_user(id: int):
    with _lock:
        data = _load()
        before = len(data["users"])
        data["users"] = [x for x in data["users"] if x["id"] != id]
        if len(data["users"]) < before:
            _save(data)
            return {"ok": True}
        raise HTTPException(404, "not found")
