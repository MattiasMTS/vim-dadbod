" Location: autoload/db/adapter/redshift.vim
" Author: Your Name

function! db#adapter#redshift#canonicalize(url) abort
  let dsn_pattern = '^redshift:\zs\<\k\+\>'
  let dsn = matchstr(a:url, dsn_pattern)
  if !empty(dsn)
    return 'rsql -D ' . dsn
  " else
  "   let url = substitute(a:url, '^[^:]*:/\=/\@!', 'redshift:///', '')
  "   return db#url#absorb_params(url, {
  "         \ 'user': 'user',
  "         \ 'password': 'password',
  "         \ 'host': 'host',
  "         \ 'port': 'port',
  "         \ 'dbname': 'dbname'})
  endif
endfunction

function! db#adapter#redshift#interactive(url, ...) abort
  let short = matchstr(a:url, '^[^:]*:\%(///\)\=\zs[^/?#]*$')
  return ['rsql', '-w'] + (a:0 ? a:1 : []) + ['--dbname', len(short) ? short : a:url]
endfunction

function! db#adapter#redshift#filter(url) abort
  return db#adapter#redshift#interactive(a:url,
        \ ['-P', 'columns=' . &columns, '-v', 'ON_ERROR_STOP=1'])
endfunction

function! db#adapter#redshift#input(url, in) abort
  return db#adapter#redshift#filter(a:url) + ['-f', a:in]
endfunction

function! s:parse_columns(output, ...) abort
  let rows = map(copy(a:output), 'split(v:val, "|")')
  if a:0
    return map(filter(rows, 'len(v:val) > a:1'), 'v:val[a:1]')
  else
    return rows
  endif
endfunction

function! db#adapter#redshift#complete_database(url) abort
  let cmd = ['rsql', '--no-psqlrc', '-ltAX', substitute(a:url, '/[^/]*$', '/postgres', '')]
  return s:parse_columns(db#systemlist(cmd), 0)
endfunction

function! db#adapter#redshift#complete_opaque(_) abort
  return db#adapter#redshift#complete_database('')
endfunction

function! db#adapter#redshift#tables(url) abort
  return s:parse_columns(db#systemlist(
        \ db#adapter#redshift#filter(a:url) + ['--no-psqlrc', '-tA', '-c', '\dtvm']), 1)
endfunction
