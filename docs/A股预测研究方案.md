# MiroFish A股预测自动化研究方案

## 一、系统理解

### MiroFish 核心架构

MiroFish 是一个**多智能体群体智能引擎**，通过以下流程实现预测：

1. **图谱构建** - 上传种子文档 → LLM生成本体(实体/关系类型) → Zep Cloud构建知识图谱
2. **环境搭建** - 从图谱提取实体 → 生成OASIS Agent画像(人格/行为参数) → LLM生成仿真参数
3. **社媒模拟** - Twitter/Reddit双平台并行模拟 → Agent自由发帖/评论/转发/点赞
4. **报告生成** - ReportAgent通过ReACT模式 → 调用图谱检索工具 → 生成多章节分析报告
5. **深度交互** - 与仿真世界中的任意Agent对话 → 与ReportAgent进行Q&A

### 技术栈

| 组件 | 技术 |
|------|------|
| 前端 | Vue 3 + D3.js + Vite |
| 后端 | Python 3.11+ Flask |
| LLM | OpenAI兼容API (推荐qwen-plus) |
| 图谱存储 | Zep Cloud |
| 仿真引擎 | OASIS (CAMEL-AI) |

### 关键配置

- **实体类型**: 每次最多10个(8个具体类型 + Person/Organization兜底)
- **关系类型**: 6-10个
- **模拟轮次**: 默认10轮，建议<40轮（消耗较大）
- **支持格式**: PDF/MD/TXT

---

## 二、A股预测研究方案

### 方案A：单公司舆情推演预测

**目标**：预测某A股公司在特定事件（如财报发布、政策变化）后的股价走势

**种子文档准备**：
```
文档1: 公司基本面分析报告 (PDF/TXT)
  - 公司简介、主营业务、财务数据
  - 近期重大事件（如收购、诉讼、政策利好/利空）
  - 行业地位和竞争格局

文档2: 市场参与者关系图谱 (TXT/MD)
  - 公司管理层（CEO/CFO/董事长）
  - 主要股东（机构投资者、散户代表）
  - 监管机构（证监会、交易所）
  - 分析师/券商研究员
  - 行业竞争对手
  - 媒体（财经媒体、自媒体大V）
  - 供应链上下游企业

文档3: 最新相关新闻汇总 (TXT)
  - 近1-4周内与该公司相关的新闻
  - 行业政策变化
  - 市场情绪指标
```

**模拟需求描述示例**：
```
模拟[公司名]在[事件描述]后的市场反应。
请预测：
1. 各方利益相关者的反应和立场
2. 市场情绪从发酵到平息的演变过程
3. 对股价的短期(1周)和中期(1个月)影响方向
4. 最可能出现的几种情景及其概率
```

**预期本体设计**：

| 实体类型 | 说明 |
|----------|------|
| ListedCompany | 上市公司 |
| Executive | 高管（CEO/CFO/董事长） |
| InstitutionalInvestor | 机构投资者（基金/保险/外资） |
| RetailInvestor | 散户投资者代表 |
| Analyst | 券商研究员/分析师 |
| Regulator | 监管机构（证监会/交易所） |
| MediaOutlet | 财经媒体 |
| Competitor | 行业竞争对手 |
| Person | 兜底个人 |
| Organization | 兜底组织 |

**预期关系类型**：
- INVESTS_IN: 投资于
- COMPETES_WITH: 竞争
- REGULATES: 监管
- ANALYZES: 分析/覆盖
- REPORTS_ON: 报道
- SUPPLIES_TO: 供应
- MANAGES: 管理
- COLLABORATES_WITH: 合作

---

### 方案B：行业板块推演预测

**目标**：预测某A股行业板块（如新能源、半导体、AI）在政策/事件驱动下的整体走势

**种子文档准备**：
```
文档1: 行业分析报告
  - 板块整体概况、市场规模
  - 主要上市公司列表及市值排名
  - 行业产业链上中下游分析
  - 技术发展趋势

文档2: 政策/事件文档
  - 最新政策原文或摘要（如补贴政策、关税调整）
  - 国际市场变化（如美股映射）
  - 技术突破新闻

文档3: 市场参与者分析
  - 头部公司及其竞争关系
  - 重要机构投资者持仓
  - 行业专家/意见领袖
  - 监管动态
```

---

### 方案C：多因子综合推演

**目标**：综合宏观经济、行业、公司三个层面进行预测

**种子文档准备**：
```
文档1: 宏观经济环境 (TXT)
  - GDP/CPI/PMI等宏观指标
  - 央行货币政策动向（降息/降准预期）
  - 人民币汇率趋势
  - 中美关系/地缘政治

文档2: 目标行业/公司深度分析 (PDF)
  - 详细的基本面数据
  - 技术面关键位分析
  - 资金面数据（北向资金、融资余额）

文档3: 市场情绪与舆论 (TXT)
  - 社交媒体热度（雪球/东方财富股吧）
  - 机构观点汇总
  - 最新传闻与市场预期
```

---

## 三、实施步骤

### Step 1: 环境搭建

```bash
# 1. 配置环境变量
cp .env.example .env

# 编辑 .env，填入：
# - LLM_API_KEY (推荐阿里百炼平台 qwen-plus)
# - LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
# - LLM_MODEL_NAME=qwen-plus
# - ZEP_API_KEY (https://app.getzep.com/ 免费额度)

# 2. 安装依赖
npm run setup:all

# 3. 启动服务
npm run dev
# 前端: http://localhost:3000
# 后端: http://localhost:5001
```

### Step 2: 准备A股种子文档

需要为目标股票/公司准备种子材料。建议**自动化数据采集**：

```python
# 需要开发的数据采集脚本 (建议新建 scripts/a_share_data_collector.py)

# 数据源建议：
# 1. 东方财富/同花顺 - 公司基本面、财务数据
# 2. 新浪财经/雪球 - 新闻、研报摘要
# 3. 巨潮资讯网 - 公告原文
# 4. Wind/Choice - 行业数据 (需付费)

# 输出格式：生成 TXT/MD 文件，包含结构化的公司/行业信息
```

### Step 3: 上传文档并运行模拟

1. 访问 `http://localhost:3000`
2. 上传准备好的种子文档
3. 输入模拟需求描述（参见上面的示例）
4. 系统自动生成本体 → 构建图谱 → 准备仿真环境
5. 运行模拟（建议首次10-20轮）
6. 生成预测报告

### Step 4: 评估与迭代

- 对比预测结果与实际走势
- 调整种子文档的信息密度和覆盖面
- 优化模拟参数（轮次、Agent数量、活跃度）

---

## 四、自动化增强建议

### 4.1 数据采集自动化

建议开发一个数据采集模块，自动从公开数据源获取A股信息：

```
scripts/
├── a_share_data_collector.py    # A股数据采集入口
├── sources/
│   ├── eastmoney.py             # 东方财富数据源
│   ├── sina_finance.py          # 新浪财经新闻
│   ├── xueqiu.py                # 雪球社区舆情
│   └── cninfo.py                # 巨潮资讯公告
├── generators/
│   ├── company_profile.py       # 公司画像文档生成
│   ├── industry_report.py       # 行业报告文档生成
│   └── news_summary.py          # 新闻汇总文档生成
└── templates/
    ├── company_template.md      # 公司分析模板
    └── industry_template.md     # 行业分析模板
```

### 4.2 API自动化调用

可以绕过前端UI，直接调用后端API实现全流程自动化：

```python
import requests

BASE_URL = "http://localhost:5001/api"

# Step 1: 上传文档 + 生成本体
resp = requests.post(f"{BASE_URL}/graph/ontology/generate",
    files=[("files", open("company_report.txt", "rb"))],
    data={"simulation_requirement": "预测XX公司股价走势..."})
project_id = resp.json()["project_id"]

# Step 2: 构建图谱
resp = requests.post(f"{BASE_URL}/graph/build",
    json={"project_id": project_id})
task_id = resp.json()["task_id"]
# 轮询等待完成...

# Step 3: 创建并准备模拟
resp = requests.post(f"{BASE_URL}/simulation/create",
    json={"project_id": project_id, "graph_id": graph_id})
simulation_id = resp.json()["simulation_id"]

resp = requests.post(f"{BASE_URL}/simulation/prepare",
    json={"simulation_id": simulation_id})
# 轮询等待完成...

# Step 4: 运行模拟
resp = requests.post(f"{BASE_URL}/simulation/run",
    json={"simulation_id": simulation_id})
# 轮询等待完成...

# Step 5: 生成报告
resp = requests.post(f"{BASE_URL}/report/generate",
    json={"simulation_id": simulation_id})
# 轮询等待完成...

# Step 6: 获取报告
resp = requests.get(f"{BASE_URL}/report/{report_id}")
report = resp.json()
```

### 4.3 批量回测框架

```python
# 思路：对历史事件进行回测验证
# 1. 选取历史上已知结果的A股事件（如某公司暴雷、政策变化）
# 2. 用事件发生前的信息作为种子文档
# 3. 运行模拟获取预测
# 4. 对比预测结果与实际走势
# 5. 统计预测准确率，优化方案
```

---

## 五、风险与局限

1. **模型局限**: MiroFish基于LLM + 社媒模拟，本质是模拟舆情传播而非量化分析。更适合预测"市场情绪方向"而非"精确涨跌幅"

2. **数据质量**: 预测质量高度依赖种子文档的质量和全面性

3. **成本考虑**:
   - LLM API调用费用（qwen-plus相对便宜）
   - Zep Cloud免费额度有限
   - 模拟轮次越多消耗越大

4. **时效性**: 股市变化快速，种子文档的时效性很关键

5. **合规提醒**:
   - 不构成投资建议
   - A股数据采集需遵守相关法规
   - 自动化交易需取得相应资质

---

## 六、优先级建议

| 优先级 | 任务 | 说明 |
|--------|------|------|
| P0 | 环境搭建 | 配置.env，安装依赖，启动服务 |
| P0 | 手动试跑 | 选一个A股公司，手动准备文档，完成一次完整流程 |
| P1 | 数据采集脚本 | 自动化获取A股公司/行业数据 |
| P1 | API自动化脚本 | 实现全流程API调用自动化 |
| P2 | 批量回测 | 对历史事件进行回测验证 |
| P2 | 结果评估体系 | 建立预测准确率评估指标 |
| P3 | 定时任务 | 每日/每周自动运行预测 |
