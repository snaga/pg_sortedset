--
-- PostgreSQL 9.5 or later required.
--
CREATE EXTENSION hstore;
CREATE EXTENSION plpythonu;
CREATE EXTENSION hstore_plpythonu;

DROP TABLE IF EXISTS z;

CREATE TABLE z (
  key TEXT PRIMARY KEY,
  member_with_score HSTORE,
  -- sorted_set must be sorted by the score.
  sorted_set TEXT[]
);


INSERT INTO z VALUES ('foo');
UPDATE z
   SET member_with_score = 'a=>1, b=>2, c=>3, d=>4, e=>5'::hstore,
       sorted_set = ARRAY['a', 'b', 'c', 'd', 'e']
 WHERE key = 'foo';

--
-- zrank
--
CREATE OR REPLACE FUNCTION zrank(key TEXT, member TEXT)
  RETURNS INTEGER AS
$$
  SELECT zrank_impl($1, $2, false);
$$ LANGUAGE SQL;

--
-- zrevrank
--
CREATE OR REPLACE FUNCTION zrevrank(key TEXT, member TEXT)
  RETURNS INTEGER AS
$$
  SELECT zrank_impl($1, $2, true);
$$ LANGUAGE SQL;

--
-- zrank_impl
--
CREATE OR REPLACE FUNCTION zrank_impl(key TEXT, member TEXT, reverse BOOLEAN)
  RETURNS INTEGER AS
$$
    import plpy

    rs = plpy.execute("SELECT member_with_score->'%s' as score,member_with_score::hstore,sorted_set FROM z WHERE key = '%s'" % (member, key))

    # key not found.
    if rs.nrows() != 1:
        return None

#    plpy.info('rs = %s' % str(rs[0]))

    score = rs[0]['score']
#    plpy.info('member = %s, score = %s' % (member, score))
    if score is None:
        # member and the score not found
        return None

    member_with_score = rs[0]['member_with_score']
#    plpy.info('member_with_store = %s' % str(member_with_score))
#    plpy.info('%s' % member_with_score['a'])

    sorted_set = rs[0]['sorted_set']
#    plpy.info('sorted_set = %s' % str(sorted_set))

    # binary search
    start = 0
    end = len(sorted_set) - 1

    # up to log2^30 entries
    for i in range(0,30):
        # floor to int
        cur = (start+end) / 2

        start_score = member_with_score[sorted_set[start]]
        start_member = sorted_set[start]
        end_score = member_with_score[sorted_set[end]]
        end_member = sorted_set[end]
        cur_score = member_with_score[sorted_set[cur]]
        cur_member = sorted_set[cur]

#        plpy.info("[%d]'%s'=%s, [%d]'%s'=%s, [%d]'%s'=%s" % (start,start_member,start_score,
#						       cur,cur_member,cur_score,
#						       end,end_member,end_score))

        if score > cur_score:
            start = cur + 1
        elif score < cur_score:
            end = cur - 1
        elif score == cur_score:
            if reverse is True:
                return len(sorted_set)-1 - cur
            else:
                return cur

    return None
$$
LANGUAGE 'plpythonu' TRANSFORM FOR TYPE hstore;

--
-- zrange
--
CREATE OR REPLACE FUNCTION zrange(key TEXT, start INTEGER, "end" INTEGER)
  RETURNS SETOF TEXT AS
$$
  SELECT zrange_impl($1, $2, $3, false);
$$ LANGUAGE SQL;

--
-- zrevrange
--
CREATE OR REPLACE FUNCTION zrevrange(key TEXT, start INTEGER, "end" INTEGER)
  RETURNS SETOF TEXT AS
$$
  SELECT zrange_impl($1, $2, $3, true);
$$ LANGUAGE SQL;

--
-- zrange_impl
--
CREATE OR REPLACE FUNCTION zrange_impl(key TEXT, start INTEGER, "end" INTEGER, reverse BOOLEAN)
  RETURNS SETOF TEXT AS
$$
    import plpy
    global start, end

    rs = plpy.execute("SELECT array_length(sorted_set,1) as set_size FROM z WHERE key = '%s'" % key)
    if rs.nrows() != 1:
        return None
    set_size = rs[0]['set_size']
    if start < 0:
        start = set_size + start
    if end < 0:
        end = set_size + end

    rs = plpy.execute("SELECT sorted_set[%d:%d] FROM z WHERE key = '%s'" % (start, end, key))

    # key not found.
    if rs.nrows() != 1:
        return None

#    plpy.info('rs = %s' % str(rs[0]))
    if reverse is True:
        members = []
        l = len(rs[0]['sorted_set'])
        for i in range(0,l):
            members.append(rs[0]['sorted_set'].pop())
    else:
        members = rs[0]['sorted_set']
    return members
$$
LANGUAGE 'plpythonu' TRANSFORM FOR TYPE hstore;

SET client_min_messages = ERROR;
SET log_min_messages = ERROR;

SELECT zrank('foo', 'a');
SELECT zrank('foo', 'b');
SELECT zrank('foo', 'c');
SELECT zrank('foo', 'd');
SELECT zrank('foo', 'e');
SELECT zrank('foo', 'f');

SELECT zrevrank('foo', 'a');
SELECT zrevrank('foo', 'b');
SELECT zrevrank('foo', 'c');
SELECT zrevrank('foo', 'd');
SELECT zrevrank('foo', 'e');
SELECT zrevrank('foo', 'f');

SELECT zrange('foo', 0, 3);
SELECT zrevrange('foo', 0, 3);
SELECT zrange('foo', 0, -1);
SELECT zrange('foo', 0, -2);
