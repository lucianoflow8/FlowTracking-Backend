import express from 'express'
import cors from 'cors'
import pkg from 'pg'
const { Pool } = pkg

const app = express()
app.use(cors())
app.use(express.json())

const db = new Pool({ connectionString: process.env.DATABASE_URL })

app.get('/', (req,res)=>res.json({ ok:true, name: process.env.APP_NAME || 'FlowTracking' }))

// Health
app.get('/health', async (req,res)=>{
  try {
    await db.query('select 1')
    res.json({ ok: true })
  } catch (e) {
    res.status(500).json({ ok:false, error: e.message })
  }
})

// Lines: mark as qr_ready (placeholder)
app.post('/lines/:id/start', async (req,res)=>{
  const { id } = req.params
  await db.query('update lines set status=$1 where id=$2', ['qr_ready', id])
  res.json({ ok: true, id, status: 'qr_ready' })
})

// Dev endpoint to simulate an incoming message
app.post('/dev/incoming', async (req,res)=>{
  const { line_id, phone, type='text', text='', media_url=null } = req.body
  await db.query(
    'insert into messages(line_id, contact_phone, msg_type, text, media_url) values($1,$2,$3,$4,$5)',
    [line_id, phone, type, text, media_url]
  )
  res.json({ ok:true })
})

// Credits price/minimum helper
app.get('/pricing', (req,res)=>{
  const price = Number(process.env.CREDIT_PRICE_USD || 3.5)
  const min = Number(process.env.MIN_CREDITS || 12)
  res.json({ unit_usd: price, min_credits: min, currency: 'USD' })
})

const port = process.env.PORT || 8080
app.listen(port, ()=>console.log('FlowTracking backend on :' + port))
