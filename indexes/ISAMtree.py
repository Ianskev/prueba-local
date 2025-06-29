import os, struct, math, re
from engine.model import TableSchema, Column, IndexType
from engine import utils
from engine import stats
from engine.record import RecordFile
import logger

_SUFIJO = re.compile(r"(.*?)(\d+)$")

def compute_string_step(min_key: str, max_key: str, slots: int) -> float:
    """
    Calcula un salto (float) uniforme entre el sufijo numérico de min_key y max_key
    para repartir 'slots' intervalos.
    """
    min_pref, min_num = _SUFIJO.match(min_key).groups()
    _,       max_num = _SUFIJO.match(max_key).groups()
    delta = int(max_num) - int(min_num)
    return max(delta / slots, 1)

def increment_string_id(key: str, offset: float) -> str:
    """
    Dado 'str10' y offset=2.5 → produce 'str12' (redondeando hacia el entero más cercano).
    """
    m = _SUFIJO.match(key)
    if not m:
        raise ValueError(f"Clave no reconoce sufijo numérico: {key}")
    prefijo, num = m.groups()
    nuevo = int(round(int(num) + offset))
    return f"{prefijo}{nuevo}"

def decrement_string_id(key: str, offset: float) -> str:
    """
    Dado un key tipo 'str10' y offset=2.5 → produce 'str8' (redondeando).
    """
    m = _SUFIJO.match(key)
    if not m:
        return key
    prefijo, num = m.groups()
    nuevo = int(round(int(num) - offset))
    if nuevo < 0:
        nuevo = 0
    return f"{prefijo}{nuevo}"

class LeafRecord:
    def __init__(self, column: Column, key, datapos: int):
        self.column = column
        self.FMT = utils.calculate_column_format(column) + "i"
        self.STRUCT = struct.Struct(self.FMT)
        self.key = key
        self.datapos = datapos

    def pack(self) -> bytes:
        val = self.key.encode() if self.column.data_type == utils.DataType.VARCHAR else self.key
        return self.STRUCT.pack(val, self.datapos)

    @staticmethod
    def unpack(column: Column, data: bytes) -> 'LeafRecord':
        FMT = utils.calculate_column_format(column) + "i"
        val, datapos = struct.unpack(FMT, data)
        if column.data_type == utils.DataType.VARCHAR:
            try:
                val = val.decode().rstrip("\x00")
            except UnicodeDecodeError:
                val = val.decode('utf-8', errors='replace').rstrip("\x00")
                print(val)
        return LeafRecord(column, val, datapos)

class IndexRecord:
    def __init__(self, column: Column, key, left: int, right: int):
        self.column = column
        self.FMT = utils.calculate_column_format(column) + "ii"
        self.STRUCT = struct.Struct(self.FMT)
        self.key = key
        self.left = left
        self.right = right

    def pack(self) -> bytes:
        val = self.key.encode() if self.column.data_type == utils.DataType.VARCHAR else self.key
        return self.STRUCT.pack(val, self.left, self.right)

    @staticmethod
    def unpack(column: Column, data: bytes) -> 'IndexRecord':
        FMT = utils.calculate_column_format(column) + "ii"
        key, left, right = struct.unpack(FMT, data)
        if column.data_type == utils.DataType.VARCHAR:
            try:
                key = key.decode().rstrip("\x00")
            except UnicodeDecodeError:
                key = key.decode('utf-8', errors='replace').rstrip("\x00")
        return IndexRecord(column, key, left, right)

class LeafPage:
    HEADER_FMT = "iii"
    HSIZE      = struct.calcsize(HEADER_FMT)

    def __init__(self, page_num:int, next_page:int, not_overflow:int, records, leaf_factor):
        self.page_num     = page_num
        self.next_page    = next_page
        self.not_overflow = not_overflow
        self.records      = records
        self.leaf_factor  = leaf_factor
        self.STRUCT       = struct.Struct(self.HEADER_FMT + "".join(r.FMT for r in records))

    def pack(self):
        hdr = (self.page_num, self.next_page, self.not_overflow)
        data = list(hdr)
        for rec in self.records:
            data.extend(rec.STRUCT.unpack(rec.pack()))
        return self.STRUCT.pack(*data)

    @classmethod
    def pack_header(cls, page_num, next_page, not_overflow):
        return struct.pack(cls.HEADER_FMT, page_num, next_page, not_overflow)


class IndexPage:
    HEADER_FMT = "i"
    HSIZE      = struct.calcsize(HEADER_FMT)

    def __init__(self, page_num, records, index_factor):
        self.page_num     = page_num
        self.records      = records
        self.index_factor = index_factor
        fmt = self.HEADER_FMT + "".join(r.FMT for r in records)
        self.STRUCT = struct.Struct(fmt)

    def pack(self):
        data = [self.page_num]
        for rec in self.records:
            data.extend(rec.STRUCT.unpack(rec.pack()))
        return self.STRUCT.pack(*data)

    def find_child_ptr(self, key):
        for rec in self.records:
            if key < rec.key:
                return rec.left
        return self.records[-1].right

class ISAMFile:
    HEADER_FMT    = "ii"
    HEADER_STRUCT = struct.Struct(HEADER_FMT)
    HEADER_SIZE   = HEADER_STRUCT.size

    def __init__(self,
                 schema: TableSchema,
                 column: Column,
                 leaf_factor: int,
                 index_factor: int):
        if column.index_type != IndexType.ISAM:
            raise Exception("column index type no coincide con ISAM")
        self.schema       = schema
        self.column       = column
        self.leaf_factor  = leaf_factor
        self.index_factor = index_factor
        self.filename     = utils.get_index_file_path(
                                schema.table_name,
                                column.name,
                                IndexType.ISAM)
        self.step = None

        if not os.path.exists(self.filename):
            open(self.filename, "wb").close()
            with open(self.filename, "r+b") as f:
                f.seek(0)
                f.write(self.HEADER_STRUCT.pack(leaf_factor, index_factor))
                stats.count_write()
        else:
            lf, ix = self.read_header()
            self.leaf_factor = lf
            self.index_factor = ix

    def read_header(self):
        with open(self.filename, "rb") as f:
            lf, ix = self.HEADER_STRUCT.unpack(f.read(self.HEADER_SIZE))
            stats.count_read()
        return lf, ix

    def _fmt_root(self):
        key_fmt = utils.calculate_column_format(self.column)
        return "i" + (key_fmt + "ii") * self.index_factor

    def _size_root(self):
        return struct.calcsize(self._fmt_root())

    def _offset_root(self):
        return self.HEADER_SIZE

    def read_root_page(self):
        buf = None
        size = self._size_root()
        with open(self.filename, "rb") as f:
            f.seek(self._offset_root())
            buf = f.read(size)
            stats.count_read()
        hdr = buf[:IndexPage.HSIZE]
        page_num = struct.unpack(IndexPage.HEADER_FMT, hdr)[0]
        records = []
        off = IndexPage.HSIZE
        for _ in range(self.index_factor):
            chunk = buf[off:off+IndexRecord(column=self.column, key=0, left=0, right=0).STRUCT.size]
            records.append(IndexRecord.unpack(self.column, chunk))
            off += IndexRecord( self.column,0,0,0).STRUCT.size
        return IndexPage(page_num, records, self.index_factor)

    def write_root_page(self, page: 'IndexPage'):
        with open(self.filename, "r+b") as f:
            f.seek(self._offset_root())
            f.write(page.pack())
            stats.count_write()

    def _offset_level1(self):
        return self.HEADER_SIZE + self._size_root()

    def read_level1_page(self, page_idx: int) -> 'IndexPage':
        lvl_size = self._size_root()
        off = self._offset_level1() + page_idx * lvl_size
        with open(self.filename, "rb") as f:
            f.seek(off)
            buf = f.read(lvl_size)
            stats.count_read()
        hdr = buf[:IndexPage.HSIZE]
        page_num = struct.unpack(IndexPage.HEADER_FMT, hdr)[0]
        recs = []
        ptr = IndexPage.HSIZE
        for _ in range(self.index_factor):
            chunk = buf[ptr:ptr+IndexRecord(self.column,0,0,0).STRUCT.size]
            recs.append(IndexRecord.unpack(self.column, chunk))
            ptr += IndexRecord(self.column,0,0,0).STRUCT.size
        return IndexPage(page_num, recs, self.index_factor)

    def write_level1_page(self, page: 'IndexPage'):
        lvl_size = self._size_root()
        off = self._offset_level1() + page.page_num * lvl_size
        with open(self.filename, "r+b") as f:
            f.seek(off)
            f.write(page.pack())
            stats.count_write()

    def _offset_leaves(self):
        return self.HEADER_SIZE + self._size_root() + self._size_root() * (1 + self.index_factor)

    def _size_leaf(self):
        key_fmt = utils.calculate_column_format(self.column)
        rec_fmt = key_fmt + "i"
        sz = struct.calcsize(LeafPage.HEADER_FMT + rec_fmt * self.leaf_factor)
        return sz

    def count_leaf_pages(self) -> int:
        """
        Returns how many leaf-pages are currently in the file.
        """
        total = os.path.getsize(self.filename)
        leaves_off = self._offset_leaves()
        leaf_sz    = self._size_leaf()
        return max(0, (total - leaves_off) // leaf_sz)

    def append_leaf_page(self, page: 'LeafPage'):
        """
        Writes the given LeafPage at the end of the file, under the
        assumption that its page_num field is already correct.
        """
        with open(self.filename, "r+b") as f:
            f.seek(0, os.SEEK_END)
            f.write(page.pack())
            stats.count_write()

    def read_leaf_page(self, leaf_idx: int) -> 'LeafPage':
        lf, ix = self.read_header()
        sz   = self._size_leaf()
        off  = self._offset_leaves() + leaf_idx * sz
        with open(self.filename, "rb") as f:
            f.seek(off)
            buf = f.read(sz)
            stats.count_read()
        pn, nxt, nof = struct.unpack(LeafPage.HEADER_FMT, buf[:LeafPage.HSIZE])
        recs = []
        ptr = LeafPage.HSIZE
        for _ in range(self.leaf_factor):
            chunk = buf[ptr:ptr+struct.calcsize(utils.calculate_column_format(self.column)+"i")]
            recs.append(LeafRecord.unpack(self.column, chunk))
            ptr += struct.calcsize(utils.calculate_column_format(self.column)+"i")
        return LeafPage(pn, nxt, nof, recs, self.leaf_factor)

    def write_leaf_page(self, page: 'LeafPage'):
        sz  = self._size_leaf()
        off = self._offset_leaves() + page.page_num * sz
        with open(self.filename, "r+b") as f:
            f.seek(off)
            f.write(page.pack())
            stats.count_write()

    def write_leaf_page_at(self, leaf_num: int,
                           records: list[LeafRecord], next_page: int,
                           not_overflow: int = None) -> None:
        """
        Rebasacribe la página hoja `leaf_num` con los datos dados.
        Si not_overflow es None, preserva el flag actual de esa página.
        """
        lf, ix = self.read_header()
        rec0 = IndexRecord(self.column, 0, 0, 0)
        record_size = rec0.STRUCT.size
        lr0 = LeafRecord(self.column, 0, 0)
        leaf_record_size = lr0.STRUCT.size

        idx_sz = IndexPage.HSIZE + ix * record_size
        leaf_off = self._offset_leaves()
        leaf_sz = LeafPage.HSIZE + lf * leaf_record_size

        if not_overflow is None:
            old = self.read_leaf_page(leaf_num)
            not_overflow = old.not_overflow if old else 0

        page = LeafPage(leaf_num, next_page, not_overflow, records, lf)
        with open(self.filename, "r+b") as f:
            f.seek(leaf_off + leaf_num * leaf_sz)
            f.write(page.pack())
            stats.count_write()

    def copy_to_leaf_records(self, rf: RecordFile):
        l = self.leaf_factor
        i = self.index_factor
        p = (i + 1) ** 2
        empty_key = utils.get_empty_value(self.column)

        col_idx = None
        for idx, col in enumerate(self.schema.columns):
            if col.name == self.column.name:
                col_idx = idx
                break
        if col_idx is None:
            raise Exception(f"Columna {self.column.name} no encontrada en el esquema")
        
        lf, ix = self.read_header()
        leaf_sz = self._size_leaf()
        leaves_off = self._offset_leaves()

        leaf_idx = 0
        reg_pages = 0
        max_pos = rf.max_id()

        if max_pos == 0:
            with open(self.filename, "r+b") as f:
                f.seek(0, os.SEEK_END)
                if f.tell() < leaves_off:
                    f.write(b'\x00' * (leaves_off - f.tell()))
                    stats.count_write()
                f.seek(leaves_off)
                while reg_pages < p:
                    chunk = [LeafRecord(self.column, empty_key, -1) for _ in range(l)]
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 1, chunk, l))
                    leaf_idx += 1
                    reg_pages += 1
            self._link_leaf_pages(leaves_off, leaf_sz, leaf_idx)
            return

        leafrecs = []
        for pos in range(max_pos):
            rec = rf.read(pos)
            if rec is None:
                continue
            key = rec.values[col_idx]
            leafrecs.append((key, pos))
        leafrecs.sort(key=lambda x: x[0])

        with open(self.filename, "r+b") as f:
            f.seek(0, os.SEEK_END)
            if f.tell() < leaves_off:
                f.write(b'\x00' * (leaves_off - f.tell()))
                stats.count_write()
            f.seek(leaves_off)

            if len(leafrecs) <= p * l:
                idx = 0
                while reg_pages < p and idx < len(leafrecs):
                    remain = len(leafrecs) - idx
                    slots = p - reg_pages
                    to_take = math.ceil(remain / slots)

                    end = idx + to_take
                    while end < len(leafrecs) and leafrecs[end][0] == leafrecs[end - 1][0]:
                        end += 1
                    window = leafrecs[idx:end]
                    idx = end
                    reg_pages += 1

                    hoja = window[:l]
                    overflow = window[l:]
                    chunk = [LeafRecord(self.column, k, dp) for k, dp in hoja]
                    while len(chunk) < l:
                        chunk.append(LeafRecord(self.column, empty_key, -1))
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 1, chunk, l))
                    leaf_idx += 1

                    for j in range(0, len(overflow), l):
                        seg = overflow[j:j + l]
                        chunk = [LeafRecord(self.column, k, dp) for k, dp in seg]
                        while len(chunk) < l:
                            chunk.append(LeafRecord(self.column, empty_key, -1))
                        self.write_leaf_page(LeafPage(leaf_idx, -1, 0, chunk, l))
                        leaf_idx += 1

                while reg_pages < p:
                    chunk = [LeafRecord(self.column, empty_key, -1) for _ in range(l)]
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 1, chunk, l))
                    leaf_idx += 1
                    reg_pages += 1

            else:
                chunk = []
                for k, dp in leafrecs:
                    chunk.append(LeafRecord(self.column, k, dp))
                    if len(chunk) == l:
                        self.write_leaf_page(LeafPage(leaf_idx, -1, 0, chunk, l))
                        leaf_idx += 1
                        chunk = []
                if chunk:
                    while len(chunk) < l:
                        chunk.append(LeafRecord(self.column, empty_key, -1))
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 0, chunk, l))
                    leaf_idx += 1

        self._link_leaf_pages(leaves_off, leaf_sz, leaf_idx)

    def _link_leaf_pages(self, leaf_off: int, leaf_sz: int, count: int):
        """
        Re-escribe en cada LeafPage su next_page para apuntar a la siguiente,
        preservando el contenido de registros tal como esté.
        """
        empty_key = utils.get_empty_value(self.column)

        for page_num in range(count):
            leaf = self.read_leaf_page(page_num)
            next_pg = page_num + 1 if page_num + 1 < count else -1
            records = leaf.records
            if len(records) < self.leaf_factor:
                padding = [
                    LeafRecord(self.column, empty_key, -1)
                    for _ in range(self.leaf_factor - len(records))
                ]
                records = records + padding
            self.write_leaf_page_at(
                page_num,
                records,
                next_pg,
                not_overflow=leaf.not_overflow
            )

    def _build_level1_phase1(self, f, ctx):
        i = ctx['i']
        p = ctx['p']
        h = ctx['h']
        l = self.leaf_factor
        empty_key = utils.get_empty_value(self.column)

        last_boundary = -1
        partial_chunk = None

        while ctx['ptrs_created'] < p and ctx['pg'] < h:
            chunk = []
            last_c = 1
            last_r = -1

            for _ in range(i):
                if ctx['ptrs_created'] >= p or ctx['pg'] >= h:
                    break

                rem_leaves = h - ctx['pg']
                rem_ptrs = p - ctx['ptrs_created']
                c = max(1, rem_leaves // rem_ptrs)

                left = ctx['pg']

                lp = self.read_leaf_page(left)

                if all(rec.key == empty_key for rec in lp.records):

                    partial_chunk = chunk
                    return last_boundary, partial_chunk

                ctx['pg'] += c
                last_c = c
                ctx['ptrs_created'] += 1


                vals = [rec.key for rec in lp.records if rec.key != empty_key]
                if vals:
                    if ctx['seen_count'] == 0:
                        ctx['min_id'] = vals[0]
                    ctx['seen_count'] += len(vals)
                    ctx['last_valid'] = vals[-1]
                    ctx['max_id'] = vals[-1]


                cand = ctx['pg'] if ctx['pg'] < h else -1
                while cand != -1:
                    nl = self.read_leaf_page(cand)
                    if nl.records[0].key == ctx['last_valid']:
                        cand = nl.next_page
                    else:
                        break
                right = cand if (cand != -1 and cand < h) else -1


                if right != -1:
                    rlp = self.read_leaf_page(right)
                    if rlp is None or all(rec.key == empty_key for rec in rlp.records):
                        partial_chunk = chunk
                        return last_boundary, partial_chunk


                if right != -1:
                    rec_id = self.read_leaf_page(right).records[0].key
                else:
                    rec_id = ctx['last_valid']


                self.write_leaf_page(LeafPage(
                    left, lp.next_page, 1, lp.records, l
                ))


                chunk.append(IndexRecord(self.column, rec_id, left, right))
                last_r = right


                if last_r != -1:
                    ctx['pg'] = last_r


            if len(chunk) < i:
                partial_chunk = chunk
                return last_boundary, partial_chunk


            if last_r != -1:

                rlp = self.read_leaf_page(last_r)

                self.write_leaf_page_at(
                    last_r,
                    rlp.records,
                    rlp.next_page,
                    not_overflow = 1
                )


            self.write_level1_page(IndexPage(ctx['page_idx'], chunk, i))
            ctx['ptrs_created'] += 1
            ctx['page_idx'] += 1


            last_boundary = last_r
            if last_r != -1:
                ctx['pg'] = last_r + last_c


        return last_boundary, None

    def _build_level1_phase2(self, ctx, last_boundary, partial_chunk=None):
        i, p, h = ctx['i'], ctx['p'], ctx['h']


        slots = p - ctx['phase1_pages']
        step = ctx['step']


        current_key = ctx['max_id']


        if partial_chunk:
            chunk = partial_chunk
            for _ in range(len(chunk), i):
                if ctx['ptrs_created'] >= p or ctx['pg'] >= h:
                    break
                left = ctx['pg']
                ctx['pg'] += 1
                ctx['ptrs_created'] += 1
                right = ctx['pg'] if ctx['pg'] < h else -1


                current_key = increment_string_id(current_key, step)
                chunk.append(IndexRecord(self.column, current_key, left, right))

            self.write_level1_page(IndexPage(ctx['page_idx'], chunk, i))
            ctx['page_idx'] += 1


        while ctx['ptrs_created'] < p and ctx['pg'] < h and ctx['page_idx'] < (i + 1):
            chunk = []
            current_key = increment_string_id(current_key, step)
            for _ in range(i):
                if ctx['ptrs_created'] >= p or ctx['pg'] >= h:
                    break
                left = ctx['pg']
                ctx['pg'] += 1
                ctx['ptrs_created'] += 1
                right = ctx['pg'] if ctx['pg'] < h else -1
                current_key = increment_string_id(current_key, step)
                print(step)
                print(current_key)
                chunk.append(IndexRecord(self.column, current_key, left, right))

            if not chunk:
                break

            self.write_level1_page(IndexPage(ctx['page_idx'], chunk, i))
            ctx['page_idx'] += 1

    def build_level1(self):
        """
        Construye el Nivel 1 completo (fases 1 y 2).
        Asume que ya has corrido copy_to_leaf_records() antes.
        """

        lf, ix = self.read_header()
        i = ix
        p = (i + 1) ** 2


        rec0 = IndexRecord(self.column, 0, 0, 0)
        record_size = rec0.STRUCT.size

        idx_sz = IndexPage.HSIZE + i * record_size
        lr0 = LeafRecord(self.column, 0, 0)
        leaf_record_size = lr0.STRUCT.size

        leaf_sz = LeafPage.HSIZE + lf * leaf_record_size
        total = os.path.getsize(self.filename)
        leaf_off = self._offset_leaves()
        h = (total - leaf_off) // leaf_sz


        level1_off = self.HEADER_SIZE + idx_sz


        ctx = {
            'i': i,
            'p': p,
            'h': h,
            'ptrs_created': 0,
            'page_idx': 0,
            'pg': 0,
            'min_id': None,
            'max_id': None,
            'seen_count': 0,
            'last_valid': None,
            'step': 0,
        }


        with open(self.filename, 'r+b') as f:
            f.seek(level1_off)



            last_boundary, partial_chunk = self._build_level1_phase1(f, ctx)


            ctx['phase1_pages'] = ctx['page_idx']



            slots = (i + 1) ** 2 - ctx['phase1_pages']

            ctx['step'] = compute_string_step(ctx['min_id'], ctx['max_id'], slots)



            self._build_level1_phase2(ctx, last_boundary, partial_chunk)


        self.num_level1 = ctx['page_idx']
        self.step = ctx['step']

    def build_root(self):
        """
        Construye la página ROOT (page_num=0) con index_factor registros,
        apuntando a las páginas de nivel1 0..index_factor. El rec_id de cada
        registro es el mínimo de la primera hoja apuntada; si esa hoja está
        “vacía” se utiliza el rec_id de nivel1 menos `self.step`.
        """

        lf, ix = self.read_header()
        i = ix
        is_string = (self.column.data_type == utils.DataType.VARCHAR)
        records: list[IndexRecord] = []

        for left in range(i):
            right = left + 1
            lvl1 = self.read_level1_page(right)
            first_leaf_pg = lvl1.records[0].left
            leaf = self.read_leaf_page(first_leaf_pg)

            if leaf is None or all(r.key == utils.get_empty_value(self.column) for r in leaf.records):
                base = lvl1.records[0].key
                if is_string:
                    rec_id = decrement_string_id(base, self.step)
                else:
                    rec_id = base - self.step
            else:
                rec_id = leaf.records[0].key

            records.append(IndexRecord(self.column, rec_id, left, right))

        root_page = IndexPage(
            page_num=0,
            records=records,
            index_factor=self.index_factor
        )
        self.write_root_page(root_page)





class ISAMIndex:
    def __init__(self,
                 schema: TableSchema,
                 column: Column,
                 leaf_factor: int | None  = None,
                 index_factor: int | None = None):
        self.schema = schema
        self.column = column
        self.rf = RecordFile(schema)




        lf_arg = leaf_factor or 0
        ix_arg = index_factor or 0
        self.file = ISAMFile(schema, column, lf_arg, ix_arg)
        lf, ix = self.file.read_header()
        self.file.leaf_factor = lf
        self.file.index_factor = ix
        self.logger = logger.CustomLogger(f"ISAMINDEX-{schema.table_name}-{column.name}")
        self.num_level1 = 0
        self.num_leaves = 0
        self.step = None

    def _calculate_factors(self, fill_factor: float = 0.5):
        """
        Ajusta self.file.leaf_factor e self.file.index_factor para que
        las hojas queden llenas en fill_factor (%) y el nivel-1 abarque todas.
        """

        N = count_records_in_rf(self.rf)


        leaf_header = LeafPage.HSIZE
        index_header = IndexPage.HSIZE
        rec_sz = LeafRecord(self.column, 0, 0).STRUCT.size
        idx_sz = IndexRecord(self.column, 0, 0, 0).STRUCT.size


        page_size = 4096
        l_max = (page_size - leaf_header) // rec_sz
        i_max = (page_size - index_header) // idx_sz


        f = fill_factor
        l = l_max
        leaf_pages_est = math.ceil(N / (l * f))


        i = min(i_max, math.ceil(math.sqrt(leaf_pages_est)) - 1)
        p = (i + 1) ** 2


        l = min(l_max, math.ceil(N / (p * f)))


        l = max(2, l)
        i = max(2, i)


        self.file.leaf_factor = l
        self.file.index_factor = i
        with open(self.file.filename, "r+b") as fh:
            fh.seek(0)
            fh.write(self.file.HEADER_STRUCT.pack(l, i))

    def build_index(self):

        self._calculate_factors(fill_factor=0.5)


        self.file.copy_to_leaf_records(self.rf)


        self.file.build_level1()


        self.file.build_root()

        print(self)

    def rangeSearch(self, ini, end) -> list[int]:
        """
        Devuelve la lista de datapos de todos los registros con key entre
        ini y end (inclusive), recorriendo hoja tras hoja y saltando valores nulos.
        """
        if ini is None:
            ini = utils.get_min_value(self.column)
        if end is None:
            end = utils.get_max_value(self.column)
        self.logger.warning(f"RANGE-SEARCH: {ini}, {end}")

        empty_key = utils.get_empty_value(self.column)
        results = []
        if end < ini:
            return results


        root = self.file.read_root_page()
        lvl1_num = root.find_child_ptr(ini)


        lvl1 = self.file.read_level1_page(lvl1_num)


        leaf_num = lvl1.find_child_ptr(ini)
        lp: LeafPage = self.file.read_leaf_page(leaf_num)


        while lp is not None:
            for rec in lp.records:

                if rec.key == empty_key:
                    continue

                if rec.key < ini:
                    continue

                if rec.key > end:
                    return results
                results.append(rec.datapos)
            if lp.next_page < 1:
                print(self)
                break
            lp = self.file.read_leaf_page(lp.next_page)

        return results

    def search(self, key) -> list[int]:
        self.logger.warning(f"SEARCHING: {key}")
        """
        Búsqueda puntual: sólo devuelve los datapos de los registros con key == key.
        Internamente llama a range_search(key, key).
        """
        return self.rangeSearch(key, key)

    def insert(self, pos: int, key: any):
        """
        1) Persistir en RecordFile.
        2) Bajar ROOT → nivel1 → hoja_base.
        3) Recorrer la cadena de hojas hasta encontrar la hoja destino.
        4) Si hay hueco, insertar ordenado.
        5) Si está llena, intentar fusionar con su overflow:
           • Si existe overflow y tiene hueco, merge en dos páginas.
           • Si no, crear un overflow simple.
        """
        self.logger.warning(f"INSERTING: {key}")
        lf = self.file.leaf_factor
        empty_key = utils.get_empty_value(self.column)


        datapos = pos
        new_lr = LeafRecord(self.column, key, datapos)


        if not hasattr(self.file, "num_leaves"):
            self.num_leaves = self.file.count_leaf_pages()


        root = self.file.read_root_page()
        lvl1_num = root.find_child_ptr(new_lr.key)
        lvl1 = self.file.read_level1_page(lvl1_num)
        leaf_base = lvl1.find_child_ptr(new_lr.key)


        prev_leaf = None
        curr_leaf = leaf_base
        while True:
            leaf = self.file.read_leaf_page(curr_leaf)

            idx = next((i for i, r in enumerate(leaf.records) if r.key > new_lr.key), None)
            if idx is not None:
                dest = curr_leaf
                break

            if leaf.next_page != -1:
                nxt = self.file.read_leaf_page(leaf.next_page)
                if nxt.not_overflow:
                    dest = curr_leaf
                    break
            else:
                dest = curr_leaf
                break
            prev_leaf = curr_leaf
            curr_leaf = leaf.next_page


        leaf_dest = self.file.read_leaf_page(dest)
        cur = [r for r in leaf_dest.records if r.key != empty_key]
        next_pg = leaf_dest.next_page

        if len(cur) < lf:

            cur.append(new_lr)
            cur.sort(key=lambda r: r.key)
            while len(cur) < lf:
                cur.append(LeafRecord(self.column, empty_key, -1))
            leaf_dest.records = cur
            leaf_dest.next_page = next_pg

            self.file.write_leaf_page(leaf_dest)
            print(f"Insertado en hoja destino #{dest}")
            return


        if next_pg != -1:
            leaf_over = self.file.read_leaf_page(next_pg)
            if not leaf_over.not_overflow:
                cur2 = [r for r in leaf_over.records if r.key != empty_key]
                if len(cur2) < lf:

                    merged = sorted(cur + cur2 + [new_lr], key=lambda r: r.key)
                    c1 = merged[:lf]
                    c2 = merged[lf:lf * 2]
                    nxt = leaf_over.next_page

                    while len(c1) < lf: c1.append(LeafRecord(self.column, empty_key, -1))
                    while len(c2) < lf: c2.append(LeafRecord(self.column, empty_key, -1))

                    leaf_dest.records = c1
                    leaf_dest.next_page = next_pg
                    self.file.write_leaf_page(leaf_dest)
                    leaf_over.records = c2
                    leaf_over.next_page = nxt
                    self.file.write_leaf_page(leaf_over)
                    print(f"Fusionado hojas {dest} + {next_pg}")
                    return


        new_leaf_id = self.num_leaves
        merged = sorted(cur + [new_lr], key=lambda r: r.key)
        c1, c2 = merged[:lf], merged[lf:]

        while len(c1) < lf: c1.append(LeafRecord(self.column, empty_key, -1))
        while len(c2) < lf: c2.append(LeafRecord(self.column, empty_key, -1))


        leaf_dest.records = c1
        leaf_dest.next_page = new_leaf_id
        self.file.write_leaf_page(leaf_dest)


        overflow = LeafPage(
            page_num=new_leaf_id,
            next_page=next_pg,
            not_overflow=False,
            records=c2,
            leaf_factor=lf
        )
        self.file.append_leaf_page(overflow)
        self.num_leaves += 1

        print(f"Overflow simple: hoja {dest} → nueva hoja {new_leaf_id}")

    def delete(self, key: any):
        self.logger.warning(f"DELETING: {key}")
        lf = self.file.leaf_factor


        root    = self.file.read_root_page()
        lvl1_id = root.find_child_ptr(key)
        lvl1    = self.file.read_level1_page(lvl1_id)
        first   = lvl1.find_child_ptr(key)


        prev = None
        curr = first
        first = None
        while curr != -1:
            lp = self.file.read_leaf_page(curr)
            if any(r.key == key for r in lp.records):
                first = curr
                break
            prev = curr
            curr = lp.next_page

        if first is None:
            print(f"No existe ningún registro con id={key}")
            return


        last   = first
        lp_last = self.file.read_leaf_page(last)
        while lp_last.next_page != -1:
            nxt    = lp_last.next_page
            cand   = self.file.read_leaf_page(nxt)
            if any(r.key == key for r in cand.records):
                last    = nxt
                lp_last = cand
            else:
                break


        if last == first:
            lp = self.file.read_leaf_page(first)

            kept = [r for r in lp.records if r.key != key]
            kept.sort(key=lambda r: r.key)
            while len(kept) < lf:
                kept.append( LeafRecord(self.column,
                                        utils.get_empty_value(self.column),
                                        -1) )


            if lp.not_overflow == 0 and all(r.key == utils.get_empty_value(self.column) for r in kept):
                if prev is not None:
                    prev_lp = self.file.read_leaf_page(prev)
                    self.file.write_leaf_page_at(prev,
                                            prev_lp.records,
                                            lp.next_page,
                                            not_overflow=None)
                print(f"Hoja overflow {first} quedó vacía y fue desconectada")
            else:

                self.file.write_leaf_page_at(first,
                                        kept,
                                        lp.next_page,
                                        not_overflow=lp.not_overflow)
                print(f"Eliminado id={key} en hoja única {first}")
            return



        if prev is not None:
            prev_lp = self.file.read_leaf_page(prev)
            self.file.write_leaf_page_at(prev,
                                    prev_lp.records,
                                    first,
                                    not_overflow=None)


        first_lp = self.file.read_leaf_page(first)
        self.file.write_leaf_page_at(first,
                                first_lp.records,
                                last,
                                not_overflow=None)


        lp_first = self.file.read_leaf_page(first)
        lp_last  = self.file.read_leaf_page(last)
        combined = ([r for r in lp_first.records if r.key != key] +
                    [r for r in lp_last.records  if r.key != key])
        combined.sort(key=lambda r: r.key)


        kept_first = combined[:lf]
        kept_last  = combined[lf:lf*2]

        empty = LeafRecord(self.column,
                           utils.get_empty_value(self.column),
                           -1)
        while len(kept_first) < lf:
            kept_first.append(empty)
        while len(kept_last) < lf:
            kept_last.append(empty)


        self.file.write_leaf_page_at(first,
                                kept_first,
                                last,
                                not_overflow=lp_first.not_overflow)
        self.file.write_leaf_page_at(last,
                                kept_last,
                                lp_last.next_page,
                                not_overflow=lp_last.not_overflow)


        if lp_last.not_overflow == 0 and all(r.key == utils.get_empty_value(self.column)
                                             for r in kept_last):
            after = lp_last.next_page
            self.file.write_leaf_page_at(first,
                                    kept_first,
                                    after,
                                    not_overflow=lp_first.not_overflow)
            print(f"Última hoja {last} quedó vacía y fue desconectada")

        print(f"Eliminado id={key} entre hojas {first}..{last}")

    def getAll(self) -> list[int]:
        self.logger.warning(f"GET ALL RECORDS")
        return self.rangeSearch(utils.get_min_value(self.column), utils.get_max_value(self.column))

    def __str__(self):
        lf, ix = self.file.read_header()
        s = []
        self.num_level1 = self.file.index_factor + 1
        s.append(f"ISAM Index on {self.schema.table_name}.{self.column.name}")
        s.append(f"Leaf factor: {lf}, Index factor: {ix}")
        s.append(f"Level-1 pages: 0..{self.num_level1 - 1}")
        s.append("")

        empty_key = utils.get_empty_value(self.column)


        s.append("---- ROOT PAGE ----")
        root = self.file.read_root_page()
        for rec in root.records:
            s.append(f"  ID={rec.key}, Left_lvl1={rec.left}, Right_lvl1={rec.right}")
        s.append("")


        s.append("---- LEVEL-1 PAGES ----")
        for lvl in range(self.num_level1):
            page = self.file.read_level1_page(lvl)
            s.append(f"Level1 ► Page #{lvl}")
            for rec in page.records:
                s.append(f"  ID={rec.key}, Left_leaf={rec.left}, Right_leaf={rec.right}")
            s.append("")


        s.append("---- LEAF PAGES ----")


        first_lvl1 = self.file.read_level1_page(0)
        leaf_num = first_lvl1.records[0].left
        visited = set()
        while leaf_num != -1 and leaf_num not in visited:
            visited.add(leaf_num)
            leaf = self.file.read_leaf_page(leaf_num)
            s.append(f"Leaf #{leaf.page_num}, next={leaf.next_page}, not_ovf={leaf.not_overflow}")
            for lr in leaf.records:
                if lr.key == empty_key:
                    continue
                s.append(f"  key={lr.key}, datapos={lr.datapos}")
            s.append("")
            leaf_num = leaf.next_page

        return "\n".join(s)

    def clear(self):
        self.logger.info("Cleaning data, removing files")
        os.remove(self.rf.filename)

def count_records_in_rf(rf):
    max_pos = rf.max_id()
    count = 0
    for pos in range(max_pos):
        rec = rf.read(pos)
        if rec is not None:
            count += 1
    return count

def test_isam_integrity(isam: ISAMIndex):
    dbg = []
    lf, ix = isam.file.read_header()
    expected_regular_pages = (ix + 1) ** 2

    try:

        root = isam.file.read_root_page()
        level1_0 = isam.file.read_level1_page(0)
        first_leaf = level1_0.records[0].left

        visited = set()
        leaf = isam.file.read_leaf_page(first_leaf)
        while leaf is not None and leaf.page_num not in visited:
            visited.add(leaf.page_num)
            if leaf.next_page == -1:
                break
            leaf = isam.file.read_leaf_page(leaf.next_page)

        dbg.append(f"DEBUG: Hojas encadenadas por next_page: {sorted(visited)}")
        count_regular_linked = 0
        for leaf_num in sorted(visited):
            lf_page = isam.file.read_leaf_page(leaf_num)
            dbg.append(f"DEBUG: Leaf #{leaf_num} (linked): not_overflow={lf_page.not_overflow}")
            if lf_page.not_overflow == 1:
                count_regular_linked += 1

        dbg.append(f"DEBUG: Hojas regulares alcanzables = {count_regular_linked}, esperadas = {expected_regular_pages}")
        if count_regular_linked != expected_regular_pages:
            raise AssertionError(f"[Error páginas regulares] Alcanzables={count_regular_linked}, Esperadas={expected_regular_pages}")


        root = isam.file.read_root_page()
        level1_pages = [isam.file.read_level1_page(i) for i in range(isam.num_level1)]
        empty_key = utils.get_empty_value(isam.column)

        def min_key_in_leaf(leaf_num):
            leaf = isam.file.read_leaf_page(leaf_num)
            keys = [rec.key for rec in leaf.records if rec.key != empty_key]
            return min(keys) if keys else None

        for rec in root.records:
            right = rec.right
            if right == -1:
                continue
            target_leaf = level1_pages[right].records[0].left
            min_key = min_key_in_leaf(target_leaf)
            if min_key is not None and rec.key != min_key:
                raise AssertionError(
                    f"[Error ROOT] rec.left→{rec.left}, rec.right→{right}, "
                    f"rec.key={rec.key}, min_key_en_leaf[{target_leaf}]={min_key}"
                )

        for idx, lvl1 in enumerate(level1_pages):
            for rec in lvl1.records:
                right_leaf = rec.right
                if right_leaf == -1:
                    continue
                min_key = min_key_in_leaf(right_leaf)
                if min_key is not None and rec.key != min_key:
                    raise AssertionError(
                        f"[Error Level1 pág {idx}] rec.left→{rec.left}, "
                        f"rec.right→{right_leaf}, rec.key={rec.key}, "
                        f"min_key_en_leaf[{right_leaf}]={min_key}"
                    )


        first_leaf = level1_pages[0].records[0].left
        visited_chain = set()
        leaf_num = first_leaf
        while leaf_num != -1:
            if leaf_num in visited_chain:
                raise AssertionError(f"[Error Ciclo] hoja #{leaf_num} ya visitada")
            visited_chain.add(leaf_num)
            leaf = isam.file.read_leaf_page(leaf_num)
            leaf_num = leaf.next_page

        dbg.append(f"DEBUG: Hojas encadenadas contadas = {len(visited_chain)}, esperadas = {count_regular_linked}")
        if len(visited_chain) < count_regular_linked:
            raise AssertionError(
                f"[Error Encadenamiento] Visitadas={len(visited_chain)}, "
                f"Regulares esperadas={count_regular_linked}"
            )


        dbg.append("DEBUG: Iniciando DFS sobre índice ROOT → Nivel1 → Hojas")
        visited_leaves = set()
        visited_lvl1 = set()

        for rec in root.records:
            for lvl1_num in (rec.left, rec.right):
                if lvl1_num == -1:
                    continue
                dbg.append(f"DEBUG: ROOT apunta a Nivel1 {lvl1_num}")
                if lvl1_num not in visited_lvl1:
                    dbg.append(f"  DEBUG: Visitando Nivel1 {lvl1_num}")
                    visited_lvl1.add(lvl1_num)

                lvl1_page = isam.file.read_level1_page(lvl1_num)
                for rec_lvl1 in lvl1_page.records:
                    for leaf_num in (rec_lvl1.left, rec_lvl1.right):
                        if leaf_num == -1:
                            continue
                        dbg.append(f"    DEBUG: Nivel1 {lvl1_num} rec.key={rec_lvl1.key} → Leaf {leaf_num}")
                        if leaf_num not in visited_leaves:
                            dbg.append(f"      DEBUG: Visitando Leaf {leaf_num}")
                            visited_leaves.add(leaf_num)
                            _ = isam.file.read_leaf_page(leaf_num)

        dbg.append(f"DEBUG: DFS completado. Hojas accesibles desde índice = {len(visited_leaves)}, hojas regulares esperadas = {count_regular_linked}")
        if len(visited_leaves) < count_regular_linked:
            raise AssertionError(
                f"[Error DFS] Hojas accesibles desde índice={len(visited_leaves)}, "
                f"Regulares esperadas={count_regular_linked}"
            )


        rf = isam.rf
        total_records = count_records_in_rf(rf)
        results = isam.rangeSearch(
            utils.get_min_value(isam.column),
            utils.get_max_value(isam.column)
        )
        if len(results) != total_records:
            raise AssertionError(
                f"[Error rangeSearch] Devuelve={len(results)}, "
                f"Registros en RF={total_records}"
            )


        print("Todas las pruebas de integridad ISAM pasaron correctamente.")

    except AssertionError as e:

        for line in dbg:
            print(line)

        raise
