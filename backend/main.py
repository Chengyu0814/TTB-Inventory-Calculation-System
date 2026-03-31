import ast
import operator
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Calculator API", version="1.0.0")

# 允許前端跨域請求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 支援的運算符（安全白名單）
ALLOWED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
    ast.Mod: operator.mod,
}


def safe_eval(node):
    """安全地遞迴計算 AST 節點，只允許數學運算"""
    if isinstance(node, ast.Expression):
        return safe_eval(node.body)
    elif isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float)):
            return node.value
        raise ValueError(f"不支援的常數類型: {type(node.value)}")
    elif isinstance(node, ast.BinOp):
        op_type = type(node.op)
        if op_type not in ALLOWED_OPERATORS:
            raise ValueError(f"不支援的運算符: {op_type.__name__}")
        left = safe_eval(node.left)
        right = safe_eval(node.right)
        if op_type == ast.Div and right == 0:
            raise ValueError("除數不能為零")
        return ALLOWED_OPERATORS[op_type](left, right)
    elif isinstance(node, ast.UnaryOp):
        op_type = type(node.op)
        if op_type not in ALLOWED_OPERATORS:
            raise ValueError(f"不支援的一元運算符: {op_type.__name__}")
        operand = safe_eval(node.operand)
        return ALLOWED_OPERATORS[op_type](operand)
    else:
        raise ValueError(f"不支援的語法節點: {type(node).__name__}")


class CalculateRequest(BaseModel):
    expression: str


class CalculateResponse(BaseModel):
    result: float
    expression: str


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Calculator API is running 🚀"}


@app.post("/calculate", response_model=CalculateResponse)
def calculate(request: CalculateRequest):
    """
    接收算式字串並回傳計算結果。
    例：{ "expression": "3 + 5 * 2" } → { "result": 13.0, "expression": "3 + 5 * 2" }
    """
    expression = request.expression.strip()

    if not expression:
        raise HTTPException(status_code=400, detail="算式不能為空")

    if len(expression) > 200:
        raise HTTPException(status_code=400, detail="算式過長（最多 200 字元）")

    try:
        tree = ast.parse(expression, mode="eval")
        result = safe_eval(tree)

        # 若結果為整數則回傳整數形式
        if isinstance(result, float) and result.is_integer():
            result = int(result)

        return CalculateResponse(result=float(result), expression=expression)

    except ZeroDivisionError:
        raise HTTPException(status_code=400, detail="除數不能為零")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SyntaxError:
        raise HTTPException(status_code=400, detail="無效的算式語法")
    except OverflowError:
        raise HTTPException(status_code=400, detail="計算結果數字過大")
    except Exception:
        raise HTTPException(status_code=400, detail="無法計算此算式，請確認輸入格式")
