# API para página admin (CRUD usuários) 


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from passlib.apache import HtpasswdFile
import secrets

# SALVAR EM /opt/users_api/admin_htpasswd.py

HTPASS = "/etc/nginx/.htpasswd"
router = APIRouter(prefix="/users", tags=["users"])

def ht():
    # autosave garante persistência imediata
    return HtpasswdFile(HTPASS, autosave=True)

class CreateUser(BaseModel):
    email: EmailStr
    password: str

class BlockReq(BaseModel):
    reason: str | None = None

class UnblockReq(BaseModel):
    new_password: str

@router.get("", response_model=list[str])
def list_users():
    # lista segura sem depender de métodos internos
    lines = ht().to_string().splitlines()
    users = [l.split(":",1)[0] for l in lines if l and not l.startswith("#") and ":" in l]
    return sorted(users)

@router.post("")
def create_user(body: CreateUser):
    h = ht()
    h.set_password(body.email, body.password)
    return {"ok": True}

@router.delete("/{email}")
def delete_user(email: str):
    h = ht()
    if not h.get_hash(email):
        raise HTTPException(404, "not found")
    h.delete(email)
    return {"ok": True}

@router.post("/{email}/block")
def block_user(email: str, body: BlockReq):
    # bloquear = rotacionar senha para valor aleatório
    h = ht()
    if not h.get_hash(email):
        raise HTTPException(404, "not found")
    h.set_password(email, secrets.token_urlsafe(48))
    return {"ok": True}

@router.post("/{email}/unblock")
def unblock_user(email: str, body: UnblockReq):
    h = ht()
    if not h.get_hash(email):
        raise HTTPException(404, "not found")
    h.set_password(email, body.new_password)
    return {"ok": True}
