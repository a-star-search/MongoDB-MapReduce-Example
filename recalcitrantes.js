//7. Recalcitrantes: listado de clientes que han visto más de una vez la misma película. Proporciona DNI, Nombre completo, cuántas películas ha visto repetidas, y cuántas repeticiones de media.
//Incluir el lapso de tiempo entre repeticiones (si la ha visto más de dos veces, el lapso de tiempo es el transcurrido entre el último visionado y el primero dividido entre las repeticiones

/*

En la solucion implementada, se genera un mapa (técnicamente es un "Object" y no "Map") de peliculas y elementos repetidos tanto para map como para reduce puesto que deben devolver la misma estructura de datos.

En un metodo "finalize", se unen los mapas para generar una lista con las peliculas repetidas, que es la estructura de datos final deseada.

Si no se hiciera ese paso en "finalize", en "map" habria que contar las repeticiones para generar una lista. "Reduce" tendria que iterar de nuevo sobre las listas
con la misma clave para unirlas, lo cual es mas ineficiente.

*/

var solo_clientes_que_han_visto_alguna_pelicula = {'Movies.0': {$exists: true}};

/*
Recibe dos parametros: nombre y apellidos tal y como son almacenados en un documento JSON de consumos TV.
Devuelve una cadena compuesta por el nombre y los apellidos (o el unico apellido en caso de que solo haya uno).
*/
var crear_nombre_completo = function(nombre_en_documento, apellidos_en_documento) {
  if(apellidos_en_documento.constructor === Array) {
    return nombre_en_documento + ' ' + apellidos_en_documento[0] + ' ' + apellidos_en_documento[1];
  }
  return nombre_en_documento + ' ' + apellidos_en_documento;
};

/*
De una lista con elementos repetidos a un objeto donde cada propiedad es un valor distinto de la lista y su valor el numero de repeticiones
*/
var lista_a_objeto = function(lista){
  let ocurrencias = {};
  for (elem of lista) {
    ocurrencias[elem] = ocurrencias[elem] ? ocurrencias[elem] + 1 : 1;
  }
  return ocurrencias;
};

/* 
Parametros de entrada: lista de objetos y, opcionalmente, minimo numero de repeticiones para que un objeto se incluya en el resultado. 
Devuelve una lista de listas propiedad-repeticiones.
*/
var crear_lista_con_repeticiones = function(lista, min_repeticiones = 0) {
  let obj = lista_a_objeto(lista);
  let lista_con_reps = [];
  for (let clave in obj) {
    let reps = obj[clave];
    if(reps > min_repeticiones){
      lista_con_reps.push([clave, reps]);
    }
  }
  return lista_con_reps;
};

var crear_lista_de_titulo_y_fecha_visionado = function(movies_subdoc) {
  let movie_titles = [];
  for(movie of movies_subdoc) {
    let title = movie.Title;
    let date = movie.Date;
    movie_titles.push({title: title, date, date});
  }
  return movie_titles;
};

var map_recalcitrantes = function() {
  let key  = this.Client.DNI;
  let nombre = this.Client.Name;
  let apellidos = this.Client.Surname;
  let nombre_completo = crear_nombre_completo(nombre, apellidos);
  let movies = this.Movies;
  let titulos_y_fecha = crear_lista_de_titulo_y_fecha_visionado(movies);
  emit(key, {nombre_completo: nombre_completo, titulos_con_fecha: titulos_y_fecha});
};

var reduce_recalcitrantes = function(dni, objetos) {
  //El DNI (clave) determina el nombre, por tanto se coge el nombre de cualquiera de las entradas
  let nombre = objetos[0].nombre_completo;
  let result = {nombre_completo: nombre, titulos_con_fecha: []};
  objetos.forEach(function(obj){
    result.titulos_con_fecha = result.titulos_con_fecha.concat(obj.titulos_con_fecha);
  });
  return ( result );
};

/*
 Input fecha como cadena en formato dd/mm/yyyy
 Output fecha como objeto Date
*/
var to_date = function(s_date) {
  let split = s_date.split('/');
  return new Date(split[2], split[1]-1, split[0]);
};

/*
 Input: array de objetos con estructura como { "title" : "Don McKay", "date" : "23/01/2016" }
 Output: un mapa de titulo a fecha mas reciente y fecha mas antigua
*/
var crear_mapa_de_titulo_a_intervalo_temporal = function(titulos_con_fecha){
  let result = new Map();
  for(titulo_y_fecha of titulos_con_fecha){
    let titulo = titulo_y_fecha.title;
    let fecha = to_date(titulo_y_fecha.date);
    if(titulo in result){
      if(fecha < result[titulo].mas_antiguo){
        result[titulo].mas_antiguo = fecha;
      } else if(fecha > result[titulo].mas_reciente){
        result[titulo].mas_reciente = fecha;
      }
    } else {
      let v = {mas_reciente: fecha, mas_antiguo: fecha}
      result[titulo] = v;
    }
  }
  return result;
};

const milliseconds_in_a_day = 86400000;

var finalizar = function(key, reducido) {
  let titulos_con_fecha = reducido.titulos_con_fecha;
  let titulos = titulos_con_fecha.map(titulo_y_fecha => titulo_y_fecha.title);
  let mapa_de_titulo_a_intervalo_temporal = crear_mapa_de_titulo_a_intervalo_temporal(titulos_con_fecha);
  let lista_con_repeticiones = crear_lista_con_repeticiones(titulos, 1);
  let lista = [];
  for(propiedad_y_repeticiones of lista_con_repeticiones) {
    let titulo = propiedad_y_repeticiones[0];
    let visualizaciones = propiedad_y_repeticiones[1];
    let visionado_mas_reciente = mapa_de_titulo_a_intervalo_temporal[titulo].mas_reciente;
    let visionado_mas_antiguo = mapa_de_titulo_a_intervalo_temporal[titulo].mas_antiguo;
    let dias = (visionado_mas_reciente.getTime() - visionado_mas_antiguo.getTime()) / milliseconds_in_a_day;
    let lapso = Math.round(dias / (visualizaciones - 1));
    lista.push({'Titulo': titulo, 
                'Lapso_Medio_Entre_Visualizaciones': lapso, 
                'Visualizaciones': visualizaciones});
  }
  let sum_reps = lista_con_repeticiones.reduce((a, b) => a + b[1], 0);
  let avg_reps = sum_reps / lista.length;
  return { Nombre_Completo: reducido.nombre_completo, 
           Numero_Peliculas_Vistas_Mas_Una_Vez: lista.length, 
           Media_Visionados_Repetidos: avg_reps,
           Titulos_Repetidos: lista};
};

db.consumos.mapReduce(map_recalcitrantes, reduce_recalcitrantes, 
  { query: solo_clientes_que_han_visto_alguna_pelicula, 
    out: 'recalcitrantes',
    finalize: finalizar,
    scope: {
       solo_clientes_que_han_visto_alguna_pelicula: solo_clientes_que_han_visto_alguna_pelicula,
       crear_nombre_completo: crear_nombre_completo,
       crear_lista_de_titulo_y_fecha_visionado: crear_lista_de_titulo_y_fecha_visionado,
       crear_lista_con_repeticiones: crear_lista_con_repeticiones,
       lista_a_objeto: lista_a_objeto,
       crear_mapa_de_titulo_a_intervalo_temporal: crear_mapa_de_titulo_a_intervalo_temporal,
       to_date: to_date,
       milliseconds_in_a_day: milliseconds_in_a_day
    }
  });