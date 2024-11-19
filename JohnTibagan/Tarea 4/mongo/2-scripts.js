// https://www.mongodb.com/try/download/compass

use("SocialNetworks");

// 2. Consultas básicas

// a) Insertar un documento nuevo:
db.userActivities.insertOne({
    "_id": "A101",
    "user_id": "U020",
    "platform": "Instagram",
    "type": "comment",
    "details": { "post_id": "P021", "content": "Nice post!" },
    "duration_seconds": 0,
    "search_term": null,
    "date": "2024-11-12T15:45:00"
  })
  

// b) Seleccionar documentos:

// todos
db.userActivities.find()
// usuario específico
db.userActivities.find({ "user_id": "U001" })


// c) Actualizar documentos:
db.userActivities.updateOne(
    { "_id": "A001" },
    { $set: { "details.content": "Updated post content!" } }
  )
  
// d) Eliminar un documento:
db.userActivities.deleteOne({ "_id": "A101" })



// 3. Consultas con filtros y operadores

// a) Buscar actividades de tipo 'reaction' con filtro:
db.userActivities.find({ "type": "reaction", "details.reaction_type": "like" })

// b) Buscar actividades de video con duración mayor a 300 segundos:
db.userActivities.find({ "type": "watch_time", "duration_seconds": { $gt: 300 } })

// c) Buscar actividades realizadas en YouTube o Facebook:
db.userActivities.find({ "platform": { $in: ["YouTube", "Facebook"] } })

// d) Buscar actividades con comentarios que contengan 'Amazing':
db.userActivities.find({ "details.content": { $regex: "Amazing", $options: "i" } })


//4. Consultas de agregación


// a) Contar el número total de actividades por tipo:
db.userActivities.aggregate([
  { $group: { _id: "$type", total: { $sum: 1 } } }
])

// b) Duración promedio de visualización de videos:
db.userActivities.aggregate([
  { $match: { "type": "watch_time" } },
  { $group: { _id: null, avgDuration: { $avg: "$duration_seconds" } } }
])

// c) Contar el número de reacciones por tipo de reacción:
db.userActivities.aggregate([
  { $match: { "type": "reaction" } },
  { $group: { _id: "$details.reaction_type", total: { $sum: 1 } } }
])

// d) Número de búsquedas realizadas por cada usuario:
db.userActivities.aggregate([
  { $match: { "type": "search" } },
  { $group: { _id: "$user_id", totalSearches: { $sum: 1 } } }
])