const fs = require('fs');
const readline = require('readline');
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
    contactPoints: ['localhost'], // Endereços IP dos nós do cluster Cassandra
    localDataCenter: 'datacenter1', // Nome do data center local
    keyspace: 'keyspace_name', // Nome do keyspace
    credentials: {
        username: 'user...',
        password: 'password...'
    }
});

const mysqlDumpFilePath = './data/dumfile.sql'; // Altere para o caminho do seu arquivo de dump gerado pelo comando mysqldump
const readStream = fs.createReadStream(mysqlDumpFilePath);
const rl = readline.createInterface({
    input: readStream,
    terminal: false,
});

var statements = []; // Array para armazenar declarações DROP, CREATE e INSERT

// Processa cada linha do arquivo de dump
let statement = '';
rl.on('line', (line) => {
    // Verifica se a linha não começa com /* ou -- e termina com ;
    if (!/^\s*\/\*/.test(line) && !/^\s*--/.test(line) && line.trim().endsWith(';')) {

        line = cleanStatment(line);
        statement += line;
        //---------------------------------------------------- Aqui trata dos INSERTS
        if (statement.includes('INSERT')) {
            const table = statement.split(' ')[2];
            const regex = /\(([^)]+)\)/g;
            const createStatment = statements.find(x => x.includes(`CREATE TABLE ${table}`));
            const cols = createStatment.split('(')[1].split(',');
            const columns = cols.filter(x => !x.includes('PRIMARY')).map(x => x.trim().split(' ')[0]);
            //console.log(table, createStatment, cols, columns)
            const qi = statement.split('VALUES');
            // Chama a função para extrair o conteúdo entre parênteses

            const extractedContent = getData(statement);  //extractContentBetweenParentheses(statement);

            // Agora, você pode iterar sobre as correspondências e processar o conteúdo
            for (const contentInsideParentheses of extractedContent) {
                let q = `${qi[0]} (${columns}) VALUES (uuid(), ${contentInsideParentheses})`;
                q = q.replace(/,,/g, ',').replace(/\\'/g, "''").replace(/[^']\)\)/g, ')');
                statements.push(q);
            }

            statement = '';
        } else {
            if (!line.startsWith('UNLOCK') && !line.startsWith('LOCK')) {
                statement = statement
                    .replace(/\bfrom\b/g, '"from"').replace(/\bto\b/g, '"to"').replace(/\border\b/g, '"order"').replace(/\btoken\b/g, '"token"') //substitui palavras reservadas em nomes de colunas obs: from diferente de FROM
                    .replace(/\bselect\b/g, '"select"').replace(/\binsert\b/g, '"insert"').replace(/\bupdate\b/g, '"update"').replace(/\bdelete\b/g, '"delete"') //substitui palavras reservadas em nomes de colunas obs: from diferente de FROM
                    .replace('id int', 'id UUID, old_id int');
                statement.includes('CREATE TABLE') ? statement = !statement.includes('PRIMARY KEY') ? statement.replace(/\)/g, ', PRIMARY KEY (id))') : statement : statement;
                statements.push(statement);
            }
            statement = '';
        }
    } else if (!/^\s*\/\*/.test(line) && !/^\s*--/.test(line)) {
        // Remove as aspas invertidas (`) da linha
        line = cleanStatment(line);
        statement += line;
    }
});

// Evento de conclusão da leitura do arquivo
rl.on('close', async () => {

    const drops = statements.filter(x => x.includes('DROP') && !x.includes('CREATE') && !x.includes('INSERT'));
    const creates = statements.filter(x => x.includes('CREATE TABLE') && !x.includes('DROP') && !x.includes('INSERT'));
    const inserts = statements.filter(x => x.includes('INSERT') && !x.includes('CREATE') && !x.includes('DROP'));
    var tq = drops.length + creates.length + inserts.length;
    var calc = (y) => {
        return 100 * y / tq;
    };
    var qi = 0;
    //return console.log(inserts)
    // Execute cada declaração no Cassandra
    console.log('----------------------------------------------------------------INICIANDO DROPS');
    for (const stmt of drops) {
        try {
            const rs = await client.execute(stmt);
            qi = qi + 1;
            console.log('Status da importacao', `${calc(qi)}%`);
        } catch (error) {
            console.error('😡 Erro ao executar consulta:', stmt, error);
            break;
        }
    }

    console.log('----------------------------------------------------------------INICIANDO CREATES');

    for (const stmt of creates) {
        try {
            const rs = await client.execute(stmt);
            qi = qi + 1;
            console.log('Status da importacao', `${calc(qi)}%`);
        } catch (error) {
            console.error('😡 Erro ao executar consulta:', stmt, error);
            break;
        }
    }

    console.log('----------------------------------------------------------------INICIANDO INSERTS');

    for (const stmt of inserts) {
        try {
            const rs = await client.execute(stmt);
            qi = qi + 1;
            console.log('Status da importacao', `${calc(qi)}%`);
        } catch (error) {
            console.error('😡 Erro ao executar consulta:', stmt, error);
            break;
        }
    }
});

var cleanStatment = (str) => {
    return str.replace(/`/g, '')
        .replace(/DEFAULT \((.*\(\)\)|'.*')/g, '').replace(/DEFAULT\s+'[^']+'/g, '').replace(/DEFAULT/g, '')
        .replace(/ NOT/g, '').replace(/ NULL/g, '').replace(/ AUTO_INCREMENT/g, '').replace(/ CURRENT_TIMESTAMP/g, '').replace(/ ON UPDATE/g, '').replace(/ COMMENT '.+?'/g, '') //remove limitacoes
        .replace(/tinyint\((\d+)\)/g, 'tinyint').replace(/tinytext/g, 'text').replace(/mediumint/g, 'int') //substitui tipos
        .replace(/decimal\([^)]+\)/g, 'double')
        .replace(/\bchar\b\((\d+)\)/g, 'text')
        .replace(/varchar\((\d+)\)/g, 'text').replace(/mediumtext/g, 'text').replace(/longtext/g, 'text').replace(/datetime/g, 'timestamp').replace(/json/g, 'text')
        .replace(/CHARACTER SET [^,]+/g, ',').replace(/CHARACTER SET [^,]+ COLLATE [^,]+,/g, ',')
        .replace(/unsigned/g, '')
        .replace(/COLLATE [^,]+/g, '')
        .replace(/UNIQUE/g, '')
        .replace(/KEY [^\(]+ \(.*\)/g, '')
        .replace(/CONSTRAINT [^\(]+ \(.*\)/g, '')
        .replace(/ON DELETE RESTRICT/g, '').replace(/ON DELETE CASCADE/g, '')
        .replace(/CASCADE,/g, '').replace(/CASCADE/g, '')
        .replace(/ENGINE=InnoDB=\d+/g, '').replace(/ENGINE=InnoDB/g, '')
        .replace(/CHARSET=[^,]+/g, '')
        .replace(/COLLATE=utf8mb4_unicode_ci/g, '')
        .replace(/ENGINE=InnoDB AUTO_INCREMENT=\d+ DEFAULT CHARSET=utf8mb3/g, '')
        .replace(/,,/g, ',').replace(/, ,/g, ',')
        .replace(/,   ;/g, ')')
        .replace(/  /g, ' ');
}

// Função para extrair o conteúdo entre parênteses
function extractContentBetweenParentheses(input) {
    const results = [];
    let stack = [];
    let start = null;

    for (let i = 0; i < input.length; i++) {
        if (input[i] === "(") {
            if (stack.length === 0) {
                start = i + 1;
            }
            stack.push(input[i]);
        } else if (input[i] === ")") {
            stack.pop();
            if (stack.length === 0) {
                results.push(input.slice(start, i));
            }
        }
    }

    return results;
}

function getData(input) {
    const terms = input.split('VALUES');
    const insertIntoTable = terms[0];
    const values = terms[1].trim().slice(0, -2).slice(1);
    const predata = values.split('),(');

    return predata;
}