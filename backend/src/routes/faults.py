from fastapi import APIRouter,Depends
from pydantic import BaseModel,Field
from src.config.database import database
from src.controllers.auth import admin
from src.services.faults import set_rule,list_rules,POINTS
from src.utils.serialize import row
router=APIRouter()
class FaultInput(BaseModel):
 point:str=Field(description='claim 取走任务 / result 保存结果 / deliver 写成交付')
 enabled:bool=True
 note:str=Field(default='',max_length=250)
@router.get('/faults')
def faults(db=Depends(database),u=Depends(admin)):
 return {'points':list(POINTS),'rules':[row(x) for x in list_rules(db)]}
@router.post('/faults')
def configure(b:FaultInput,db=Depends(database),u=Depends(admin)):
 return row(set_rule(db,b.point,b.enabled,b.note,u.email))
@router.post('/faults/reset')
def reset(db=Depends(database),u=Depends(admin)):
 from src.models.run import FaultRule
 for rule in db.query(FaultRule).all():
  rule.enabled=False
 db.commit()
 return {'rules':[row(x) for x in list_rules(db)]}
