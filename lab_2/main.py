import ast
import re

TOKEN_SPEC = [
    ("FUNC", r"\bfunc\b"),
    ("RETURN", r"\breturn\b"),
    ("IF", r"\bif\b"),
    ("ELSE", r"\belse\b"),
    ("FOR", r"\bfor\b"),
    ("INT", r"\bint\b"),
    ("IDENT", r"[A-Za-z_]\w*"),
    ("NUMBER", r"\d+"),
    ("PLUS", r"\+"),
    ("MINUS", r"-"),
    ("STAR", r"\*"),
    ("SLASH", r"/"),
    ("EQ", r"="),
    ("COLON_EQ", r":="),
    ("LPAREN", r"\("),
    ("RPAREN", r"\)"),
    ("LBRACE", r"\{"),
    ("RBRACE", r"\}"),
    ("COMMA", r","),
    ("SKIP", r"[ \t\n]+"),
]

TOKEN_RE = re.compile("|".join(f"(?P<{n}>{p})" for n, p in TOKEN_SPEC))


class Token:
    def __init__(self, typ, val):
        self.type = typ
        self.value = val

    def __repr__(self):
        return f"{self.type}({self.value})"


def tokenize(code):
    for m in TOKEN_RE.finditer(code):
        if m.lastgroup != "SKIP":
            yield Token(m.lastgroup, m.group())

class Node: pass

class Function(Node):
    def __init__(self, name, params, body):
        self.name = name
        self.params = params
        self.body = body

class Return(Node):
    def __init__(self, value):
        self.value = value

class Assign(Node):
    def __init__(self, name, value):
        self.name = name
        self.value = value

class If(Node):
    def __init__(self, cond, then, otherwise=None):
        self.cond = cond
        self.then = then
        self.otherwise = otherwise

class For(Node):
    def __init__(self, cond, body):
        self.cond = cond
        self.body = body

class Call(Node):
    def __init__(self, name, args):
        self.name = name
        self.args = args

class BinaryOp(Node):
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

class Var(Node):
    def __init__(self, name):
        self.name = name

class Num(Node):
    def __init__(self, value):
        self.value = value

class Parser:
    def __init__(self, tokens):
        self.tokens = list(tokens)
        self.pos = 0

    def cur(self):
        return self.tokens[self.pos]

    def eat(self, typ):
        if self.cur().type == typ:
            self.pos += 1
        else:
            raise SyntaxError(f"Expected {typ}, got {self.cur()}")

    def parse_expr(self):
        node = self.parse_term()
        while self.cur().type in ("PLUS", "MINUS"):
            op = self.cur().value
            self.eat(self.cur().type)
            node = BinaryOp(node, op, self.parse_term())
        return node

    def parse_term(self):
        tok = self.cur()
        if tok.type == "NUMBER":
            self.eat("NUMBER")
            return Num(int(tok.value))
        if tok.type == "IDENT":
            self.eat("IDENT")
            if self.cur().type == "LPAREN":
                self.eat("LPAREN")
                args = []
                if self.cur().type != "RPAREN":
                    args.append(self.parse_expr())
                    while self.cur().type == "COMMA":
                        self.eat("COMMA")
                        args.append(self.parse_expr())
                self.eat("RPAREN")
                return Call(tok.value, args)
            return Var(tok.value)
        raise SyntaxError(tok)

    def parse_stmt(self):
        if self.cur().type == "RETURN":
            self.eat("RETURN")
            return Return(self.parse_expr())

        if self.cur().type == "IF":
            self.eat("IF")
            cond = self.parse_expr()
            self.eat("LBRACE")
            then = self.parse_block()
            self.eat("RBRACE")
            otherwise = None
            if self.cur().type == "ELSE":
                self.eat("ELSE")
                self.eat("LBRACE")
                otherwise = self.parse_block()
                self.eat("RBRACE")
            return If(cond, then, otherwise)

        if self.cur().type == "FOR":
            self.eat("FOR")
            cond = self.parse_expr()
            self.eat("LBRACE")
            body = self.parse_block()
            self.eat("RBRACE")
            return For(cond, body)

        if self.cur().type == "IDENT":
            name = self.cur().value
            self.eat("IDENT")
            self.eat("EQ")
            return Assign(name, self.parse_expr())

        raise SyntaxError(self.cur())

    def parse_block(self):
        stmts = []
        while self.cur().type != "RBRACE":
            stmts.append(self.parse_stmt())
        return stmts

    def parse_func(self):
        self.eat("FUNC")
        name = self.cur().value
        self.eat("IDENT")
        self.eat("LPAREN")
        params = []
        while self.cur().type != "RPAREN":
            params.append(self.cur().value)
            self.eat("IDENT")
            if self.cur().type == "INT":
                self.eat("INT")
            if self.cur().type == "COMMA":
                self.eat("COMMA")
        self.eat("RPAREN")
        if self.cur().type == "INT":
            self.eat("INT")
        self.eat("LBRACE")
        body = self.parse_block()
        self.eat("RBRACE")
        return Function(name, params, body)

class ToPython:
    def visit(self, n):
        return getattr(self, "visit_" + n.__class__.__name__)(n)

    def visit_Function(self, n):
        return ast.FunctionDef(
            name=n.name,
            args=ast.arguments(posonlyargs=[], args=[ast.arg(p) for p in n.params],
                               kwonlyargs=[], kw_defaults=[], defaults=[]),
            body=[self.visit(s) for s in n.body],
            decorator_list=[]
        )

    def visit_Return(self, n):
        return ast.Return(self.visit(n.value))

    def visit_Assign(self, n):
        return ast.Assign([ast.Name(n.name, ast.Store())], self.visit(n.value))

    def visit_If(self, n):
        return ast.If(
            self.visit(n.cond),
            [self.visit(s) for s in n.then],
            [self.visit(s) for s in n.otherwise] if n.otherwise else []
        )

    def visit_For(self, n):
        return ast.While(self.visit(n.cond), [self.visit(s) for s in n.body], [])

    def visit_Call(self, n):
        return ast.Expr(ast.Call(ast.Name(n.name, ast.Load()),
                                 [self.visit(a) for a in n.args], []))

    def visit_BinaryOp(self, n):
        ops = {"+": ast.Add(), "-": ast.Sub(), "*": ast.Mult(), "/": ast.Div()}
        return ast.BinOp(self.visit(n.left), ops[n.op], self.visit(n.right))

    def visit_Var(self, n):
        return ast.Name(n.name, ast.Load())

    def visit_Num(self, n):
        return ast.Constant(n.value)

if __name__ == "__main__":
    go_code = """
    func add(a int, b int) int {
        if a {
            return a + b
        } else {
            return b
        }
    }
    """

    tokens = tokenize(go_code)
    parser = Parser(tokens)
    ast_go = parser.parse_func()

    py_ast = ToPython().visit(ast_go)
    mod = ast.Module([py_ast], [])
    ast.fix_missing_locations(mod)

    print(ast.unparse(mod))
