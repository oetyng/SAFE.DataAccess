using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class DataTree
    {
        IMd _head;
        IMd _currentLeaf;
        Func<MdLocator, Task> _onHeadAddressChange;

        public MdLocator MdLocator => _head.MdLocator;

        public DataTree(IMd head, Func<MdLocator, Task> onHeadAddressChange)
        {
            _head = head;
            _onHeadAddressChange = onHeadAddressChange;
        }

        public async Task<Result<Pointer>> AddAsync(string key, StoredValue value)
        {
            if (_head.IsFull)
            {
                // create new head, add pointer to current head in to it.
                // the level > 0 indicates its role as pointer holder
                var newHead = await MdAccess.CreateAsync(_head.Level + 1).ConfigureAwait(false);
                var pointer = new Pointer
                {
                    MdLocator = _head.MdLocator,
                    ValueType = typeof(Pointer).Name
                };
                await newHead.AddAsync(pointer).ConfigureAwait(false);
                _head = newHead;
                await _onHeadAddressChange(newHead.MdLocator).ConfigureAwait(false);
            }

            return await DirectlyAddToLeaf(key, value).ConfigureAwait(false);
        }

        async Task<Result<Pointer>> DirectlyAddToLeaf(string key, StoredValue value)
        {
            if (_currentLeaf == null || _currentLeaf.IsFull)
            {
                var result = await _head.AddAsync(key, value).ConfigureAwait(false);
                var leafResult = await MdAccess.LocateAsync(result.Value.MdLocator);
                if (leafResult.HasValue)
                    _currentLeaf = leafResult.Value;
                return result;
            }
            else
                return await _currentLeaf.AddAsync(key, value);
        }

        public Task<IEnumerable<StoredValue>> GetAllValuesAsync()
        {
            return _head.GetAllValuesAsync();
        }

        public Task<IEnumerable<(Pointer, StoredValue)>> GetAllPointerValuesAsync()
        {
            return _head.GetAllPointerValuesAsync();
        }
    }
}
